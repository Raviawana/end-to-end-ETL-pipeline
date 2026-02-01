import json
from pyspark.sql.functions import (
    col,
    current_timestamp
)

from utils.logger import get_logger
from utils.sparksession import create_spark_session
from utils.schema import YFINANCE_SCHEMA_MAP


def run(config_path: str):
    """
    Bronze ingestion for YFinance CSV data
    """

    # =========================
    # INIT LOGGER & SPARK
    # =========================
    logger = get_logger("ds2b_yfinance_bronze")
    spark = create_spark_session("DS2B | YFinance Bronze")

    logger.info("Spark session initialised")

    # =========================
    # LOAD CONFIG
    # =========================
    logger.info(f"Loading config from: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    CATALOG = config["catalog"]
    SCHEMA = config["schema"]
    BASE_PATH = config["base_path"]
    TABLES = config["tables"]

    logger.info(
        f"Config loaded | Catalog={CATALOG}, Schema={SCHEMA}, BasePath={BASE_PATH}"
    )

    # =========================
    # PROCESS TABLES
    # =========================
    for table in TABLES:

        table_name = table["name"]
        relative_path = table["path"]

        logger.info(f"Processing table: {table_name}")

        schema = YFINANCE_SCHEMA_MAP[table_name]

        df = (
            spark.read
            .format("csv")
            .schema(schema)
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .load(f"{BASE_PATH}/{relative_path}/*.csv")
            .withColumn("file_path", col("_metadata.file_path"))
            .withColumn("last_updated_ts", current_timestamp())
        )

        logger.info(f"Read completed for {table_name}")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"`{CATALOG}`.{SCHEMA}.{table_name}")
        )

        logger.info(f"Written Bronze table: {table_name}")

    logger.info("DS2B YFinance Bronze pipeline completed successfully")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise ValueError("Usage: python ds2b_yfinance.py <config_path>")
    run(sys.argv[1])