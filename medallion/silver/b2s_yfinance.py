import json
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from delta.tables import DeltaTable

from utils.logger import get_logger
from utils.sparksession import create_spark_session


def run(config_path: str):
    """
    Bronze → Silver SCD Type 2 pipeline for YFinance datasets
    """

    # =========================
    # INIT LOGGER & SPARK
    # =========================
    logger = get_logger("b2s_yfinance_scd2")
    spark = create_spark_session("B2S | YFinance SCD2")

    logger.info("Spark session initialised")

    # =========================
    # LOAD CONFIG
    # =========================
    logger.info(f"Loading config from: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    CATALOG = config["catalog"]
    BRONZE_SCHEMA = config["bronze_schema"]
    SILVER_SCHEMA = config["silver_schema"]
    TABLES = config["tables"]

    # =========================
    # UTILS
    # =========================
    def table_exists(table_name: str) -> bool:
        try:
            spark.table(table_name)
            return True
        except Exception:
            return False

    def ensure_scd_columns(table_name: str):
        existing_cols = {c.name for c in spark.table(table_name).schema}

        alters = []
        if "row_hash" not in existing_cols:
            alters.append("ADD COLUMN row_hash STRING")
        if "effective_from" not in existing_cols:
            alters.append("ADD COLUMN effective_from DATE")
        if "effective_to" not in existing_cols:
            alters.append("ADD COLUMN effective_to DATE")
        if "is_current" not in existing_cols:
            alters.append("ADD COLUMN is_current BOOLEAN")

        if alters:
            spark.sql(f"ALTER TABLE {table_name} {' '.join(alters)}")
            logger.info(f"Added missing SCD columns to {table_name}")

    # =========================
    # PROCESS TABLES
    # =========================
    for table in TABLES:

        table_name = table["name"]
        business_keys = table["business_key"]
        tracked_columns = table["tracked_columns"]
        hash_column = table["hash_column"]

        logger.info(f"Processing table: {table_name}")

        df = spark.table(f"`{CATALOG}`.{BRONZE_SCHEMA}.{table_name}")

        # =====================================================
        # 1️⃣ STRUCTURAL DATA QUALITY RULES
        # =====================================================
        for key in business_keys:
            df = df.filter(F.col(key).isNotNull())

        # =====================================================
        # 2️⃣ BUSINESS VALIDITY RULES (NUMERIC FIELDS)
        # =====================================================
        numeric_types = ("int", "bigint", "double", "float", "decimal")

        numeric_columns = [
            field.name
            for field in df.schema.fields
            if any(t in field.dataType.simpleString() for t in numeric_types)
        ]

        for col_name in numeric_columns:
            df = df.filter(
                F.col(col_name).isNull() |
                (F.expr(f"try_cast({col_name} as double)") >= 0)
            )

        # =====================================================
        # 3️⃣ HASH FOR SCD TYPE 2
        # =====================================================
        df = df.withColumn(
            hash_column,
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.col(c).cast("string") for c in tracked_columns]
                ),
                256
            )
        )

        target_table = f"`{CATALOG}`.{SILVER_SCHEMA}.{table_name}"

        # =====================================================
        # FIRST RUN → CREATE SILVER TABLE
        # =====================================================
        if not table_exists(target_table):
            logger.info(f"Creating Silver table: {target_table}")

            (
                df.withColumn("effective_from", F.current_date())
                  .withColumn("effective_to", F.lit(None).cast(DateType()))
                  .withColumn("is_current", F.lit(True))
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .saveAsTable(target_table)
            )
            continue

        # =====================================================
        # ENSURE SCD COLUMNS
        # =====================================================
        ensure_scd_columns(target_table)
        delta_table = DeltaTable.forName(spark, target_table)

        # =====================================================
        # BUILD MERGE CONDITION (COMPOSITE BUSINESS KEY)
        # =====================================================
        merge_condition = (
            " AND ".join([f"t.{k} = s.{k}" for k in business_keys])
            + " AND t.is_current = true"
        )

        # =====================================================
        # 4️⃣ EXPIRE CHANGED ROWS
        # =====================================================
        delta_table.alias("t") \
            .merge(df.alias("s"), merge_condition) \
            .whenMatchedUpdate(
                condition=f"t.{hash_column} <> s.{hash_column}",
                set={
                    "effective_to": "current_date()",
                    "is_current": "false"
                }
            ) \
            .execute()

        # =====================================================
        # 5️⃣ INSERT NEW / CHANGED ROWS
        # =====================================================
        silver_df = spark.table(target_table)

        join_expr = (
            [df[k] == silver_df[k] for k in business_keys] +
            [silver_df.is_current == True]
        )

        new_rows_df = df.alias("s").join(
            silver_df.alias("t"),
            join_expr,
            "left_anti"
        )

        (
            new_rows_df
            .withColumn("effective_from", F.current_date())
            .withColumn("effective_to", F.lit(None).cast(DateType()))
            .withColumn("is_current", F.lit(True))
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

        logger.info(f"SCD Type 2 completed for {table_name}")

    logger.info("B2S YFinance SCD2 pipeline completed successfully")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise ValueError("Usage: python b2s_yfinance_scd2.py <config_path>")
    run(sys.argv[1])