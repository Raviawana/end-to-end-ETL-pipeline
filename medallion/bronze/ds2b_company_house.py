import json
from pyspark.sql.functions import (
    col,
    regexp_extract,
    explode,
    current_timestamp
)

# =========================
# IMPORT SHARED UTILITIES
# =========================
from utils.logger import get_logger
from utils.sparksession import create_spark_session
from utils.schema import SCHEMA_MAP


def run(config_path: str):
    """
    Bronze ingestion for Companies House data
    """

    # =========================
    # INITIALISE LOGGER & SPARK
    # =========================
    logger = get_logger("ds2b_company_house_bronze")
    spark = create_spark_session("DS2B | Company House Bronze")

    logger.info("Spark session created successfully")

    # =========================
    # LOAD CONFIG
    # =========================
    logger.info(f"Loading config from: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    CATALOG = config["catalog"]
    SCHEMA = config["schema"]
    BASE_PATH = config["base_path"]

    logger.info(
        f"Config loaded | Catalog={CATALOG}, Schema={SCHEMA}, BasePath={BASE_PATH}"
    )

    # =========================
    # PROCESS TABLES
    # =========================
    for table in config["tables"]:

        table_name = table["name"]
        file_name = table["file"]
        explode_flag = table.get("explode", False)
        explode_column = table.get("explode_column")

        logger.info(f"Starting processing for table: {table_name}")

        df = (
            spark.read
            .schema(SCHEMA_MAP[table_name])
            .option("multiline", "true")
            .json(f"{BASE_PATH}/*/*/*/*/{file_name}")
            .withColumn("file_path", col("_metadata.file_path"))
            .withColumn(
                "company_number",
                regexp_extract("file_path", r"/([0-9A-Z]+)/[^/]+$", 1)
            )
        )

        logger.info(f"Read completed for {table_name}")

        # =========================
        # HANDLE NESTED STRUCTURES
        # =========================
        if explode_flag:
            logger.info(
                f"Exploding column '{explode_column}' for table {table_name}"
            )
            df = (
                df.withColumn("exploded", explode(col(explode_column)))
                  .select("company_number", "exploded.*")
            )

        # =========================
        # ADD METADATA
        # =========================
        df = df.withColumn("last_updated_ts", current_timestamp())

        logger.info(f"Writing Delta table: {CATALOG}.{SCHEMA}.{table_name}")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"`{CATALOG}`.{SCHEMA}.{table_name}")
        )

        logger.info(f"Completed table: {table_name}")

    logger.info("Metadata-Driven Bronze Pipeline Completed Successfully")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise ValueError("Usage: python ds2b_company_house.py <config_path>")
    run(sys.argv[1])