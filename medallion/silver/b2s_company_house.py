import json
from pyspark.sql.functions import (
    col,
    trim,
    to_date,
    initcap,
    count,
    countDistinct,
    max,
    year,
    current_date,
    when,
    lit,
    current_timestamp
)

from utils.logger import get_logger
from utils.sparksession import create_spark_session


def run(config_path: str):
    """
    Silver transformation: Build company_master from Companies House data
    """

    # =========================
    # INITIALISE LOGGER & SPARK
    # =========================
    logger = get_logger("b2s_company_master_silver")
    spark = create_spark_session("B2S | Company Master Silver")

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

    logger.info(
        f"Config loaded | Catalog={CATALOG}, Bronze={BRONZE_SCHEMA}, Silver={SILVER_SCHEMA}"
    )

    # =========================
    # READ BRONZE TABLES
    # =========================
    logger.info("Reading Bronze tables")

    overview_b = spark.table(f"`{CATALOG}`.{BRONZE_SCHEMA}.overview")
    officers_b = spark.table(f"`{CATALOG}`.{BRONZE_SCHEMA}.officers")
    filing_b = spark.table(f"`{CATALOG}`.{BRONZE_SCHEMA}.filing_history")

    # =========================
    # CLEAN & TRANSFORM OVERVIEW
    # =========================
    logger.info("Transforming overview data")

    overview_c = (
        overview_b
        .filter(col("company_number").isNotNull())
        .withColumn("company_name", trim(col("company_name")))
        .withColumn("date_of_creation", to_date("date_of_creation"))
        .withColumn("company_status", initcap(col("company_status")))
        .dropDuplicates(["company_number"])
    )

    # =========================
    # OFFICER AGGREGATION
    # =========================
    logger.info("Aggregating officers data")

    officer_summary = (
        officers_b
        .groupBy("company_number")
        .agg(
            count("*").alias("officer_count"),
            countDistinct("officer_role").alias("unique_roles")
        )
    )

    # =========================
    # FILING AGGREGATION
    # =========================
    logger.info("Aggregating filing history data")

    filing_summary = (
        filing_b
        .withColumn("date", to_date("date"))
        .groupBy("company_number")
        .agg(
            max("date").alias("last_filing_date"),
            count("*").alias("filing_count")
        )
    )

    # =========================
    # BUILD COMPANY MASTER
    # =========================
    logger.info("Joining datasets to build company master")

    company_master = (
        overview_c
        .join(officer_summary, "company_number", "left")
        .join(filing_summary, "company_number", "left")
        .withColumn(
            "company_age",
            year(current_date()) - year(col("date_of_creation"))
        )
        .withColumn(
            "is_active",
            when(col("company_status") == "Active", lit(True)).otherwise(lit(False))
        )
        .withColumn("last_updated_ts", current_timestamp())
        .filter(col("company_age") >= 0)
    )

    # =========================
    # WRITE SILVER TABLE
    # =========================
    logger.info(
        f"Writing Silver table: {CATALOG}.{SILVER_SCHEMA}.company_master"
    )

    (
        company_master.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("company_status")
        .saveAsTable(f"`{CATALOG}`.{SILVER_SCHEMA}.company_master")
    )

    logger.info("B2S Company Master Silver pipeline completed successfully")



if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise ValueError("Usage: python b2s_company_house.py <config_path>")
    run(sys.argv[1])