from pyspark.sql import SparkSession

def create_spark_session(app_name:str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enable", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark