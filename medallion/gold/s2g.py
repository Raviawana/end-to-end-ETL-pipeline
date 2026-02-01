import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.logger import get_logger
from utils.sparksession import create_spark_session


def run(config_path: str):
    """
    Silver → Gold layer pipeline
    - Promotes curated tables
    - Builds dimensions (latest snapshot)
    - Builds facts with KPIs
    """

    # =====================================
    # INIT LOGGER & SPARK
    # =====================================
    logger = get_logger("s2g_gold_layer")
    spark = create_spark_session("S2G | Gold Layer")

    logger.info("Spark session initialised")

    # =====================================
    # LOAD CONFIG
    # =====================================
    logger.info(f"Loading config from: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    CATALOG = config["catalog"]
    SILVER = config["silver_schema"]
    GOLD = config["gold_schema"]

    PROMOTE_TABLES = config.get("promote_tables", [])
    DIMENSIONS = config.get("dimensions", [])
    FACTS = config.get("facts", [])

    logger.info(f"Promote tables: {PROMOTE_TABLES}")
    logger.info(f"Dimensions: {[d['name'] for d in DIMENSIONS]}")
    logger.info(f"Facts: {[f['name'] for f in FACTS]}")

    # =====================================
    # UTILS
    # =====================================
    def drop_technical_columns(df):
        technical_cols = ["file_path", "file_name", "last_updated_ts"]
        for c in technical_cols:
            if c in df.columns:
                df = df.drop(c)
        return df

    # =====================================================
    # 1️⃣ GENERIC SILVER → GOLD PROMOTION
    # =====================================================
    for table in PROMOTE_TABLES:
        logger.info(f"Promoting table: {table}")

        df = spark.table(f"`{CATALOG}`.{SILVER}.{table}")
        df = drop_technical_columns(df)

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"`{CATALOG}`.{GOLD}.{table}")
        )

        logger.info(f"Promoted table: {table}")

    # =====================================================
    # 2️⃣ DIMENSIONS (LATEST STATE)
    # =====================================================
    for dim in DIMENSIONS:
        dim_name = dim["name"]
        source_table = dim["source_table"]

        logger.info(f"Creating dimension: {dim_name}")

        df = spark.table(f"`{CATALOG}`.{SILVER}.{source_table}")

        # Only filter SCD tables
        if "is_current" in df.columns:
            df = df.filter(F.col("is_current") == True)

        df = drop_technical_columns(df)

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"`{CATALOG}`.{GOLD}.{dim_name}")
        )

        spark.sql(f"""
            OPTIMIZE `{CATALOG}`.{GOLD}.{dim_name}
            ZORDER BY (company_number)
        """)

        logger.info(f"Dimension created: {dim_name}")

    # =====================================================
    # 3️⃣ FACTS + KPIs
    # =====================================================
    for fact in FACTS:
        fact_name = fact["name"]
        source_table = fact["source_table"]
        date_col = fact["date_column"]
        partition_cols = fact["partition_by"]

        logger.info(f"Creating fact: {fact_name}")

        df = spark.table(f"`{CATALOG}`.{SILVER}.{source_table}")

        # Facts usually come from SCD tables
        if "is_current" in df.columns:
            df = df.filter(F.col("is_current") == True)

        df = drop_technical_columns(df)

        # KPI logic (only where it makes sense)
        if fact_name == "fact_fundamentals":
            window_q = Window.partitionBy("company_number").orderBy(date_col)

            df = (
                df.withColumn(
                    "revenue_qoq_growth",
                    (F.col("total_revenue") - F.lag("total_revenue").over(window_q)) /
                    F.lag("total_revenue").over(window_q)
                )
                .withColumn(
                    "ebitda_margin",
                    F.when(
                        F.col("total_revenue") > 0,
                        F.col("ebitda") / F.col("total_revenue")
                    )
                )
            )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .saveAsTable(f"`{CATALOG}`.{GOLD}.{fact_name}")
        )

        spark.sql(f"""
            OPTIMIZE `{CATALOG}`.{GOLD}.{fact_name}
            ZORDER BY (company_number)
        """)

        logger.info(f"Fact created: {fact_name}")

    logger.info("S2G Gold pipeline completed successfully")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise ValueError("Usage: python s2g_gold_layer.py <config_path>")
    run(sys.argv[1])