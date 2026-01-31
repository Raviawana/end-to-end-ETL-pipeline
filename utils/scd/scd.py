from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Optional


class SCD:
    """
    Reusable SCD utility for Databricks Delta tables
    Supports SCD Type 1, Type 2 (hash-based), Type 3
    """

    def __init__(
        self,
        spark,
        target_table: str,
        business_key: str,
        tracked_columns: List[str],
        hash_column: Optional[str] = None,
        surrogate_key: Optional[str] = None,
        start_date_col: str = "effective_from",
        end_date_col: str = "effective_to",
        current_flag_col: str = "is_current"
    ):
        self.spark = spark
        self.target_table = target_table
        self.business_key = business_key
        self.tracked_columns = tracked_columns
        self.hash_column = hash_column
        self.surrogate_key = surrogate_key
        self.start_date_col = start_date_col
        self.end_date_col = end_date_col
        self.current_flag_col = current_flag_col

        self.delta_table = DeltaTable.forName(spark, target_table)

    # =====================================================
    # SCD TYPE 1
    # =====================================================
    def scd1(self, source_df: DataFrame):
        """
        Overwrite data, no history
        """

        update_expr = {c: f"s.{c}" for c in self.tracked_columns}

        insert_expr = {
            self.business_key: f"s.{self.business_key}",
            **update_expr
        }

        self.delta_table.alias("t") \
            .merge(
                source_df.alias("s"),
                f"t.{self.business_key} = s.{self.business_key}"
            ) \
            .whenMatchedUpdate(set=update_expr) \
            .whenNotMatchedInsert(values=insert_expr) \
            .execute()

    # =====================================================
    # SCD TYPE 2 (HASH BASED)
    # =====================================================
    def scd2(self, source_df: DataFrame):
        """
        Hash-based SCD Type 2
        Requires hash_column
        """

        if not self.hash_column:
            raise ValueError("hash_column is required for SCD Type 2")

        # 1. Expire changed current records
        self.delta_table.alias("t") \
            .merge(
                source_df.alias("s"),
                f"""
                t.{self.business_key} = s.{self.business_key}
                AND t.{self.current_flag_col} = true
                """
            ) \
            .whenMatchedUpdate(
                condition=f"t.{self.hash_column} <> s.{self.hash_column}",
                set={
                    self.end_date_col: "current_date()",
                    self.current_flag_col: "false"
                }
            ) \
            .execute()

        # 2. Insert new & changed records
        dim_df = self.spark.table(self.target_table)

        new_rows_df = source_df.alias("s") \
            .join(
                dim_df.alias("t"),
                (F.col(f"s.{self.business_key}") == F.col(f"t.{self.business_key}")) &
                (F.col(f"t.{self.current_flag_col}") == F.lit(True)),
                "left_anti"
            )

        new_rows_df \
            .withColumn(self.start_date_col, F.current_date()) \
            .withColumn(self.end_date_col, F.lit(None).cast("date")) \
            .withColumn(self.current_flag_col, F.lit(True)) \
            .write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.target_table)

    # =====================================================
    # SCD TYPE 3
    # =====================================================
    def scd3(
        self,
        source_df: DataFrame,
        current_col: str,
        previous_col: str
    ):
        """
        Limited history (current + previous)
        """

        self.delta_table.alias("t") \
            .merge(
                source_df.alias("s"),
                f"t.{self.business_key} = s.{self.business_key}"
            ) \
            .whenMatchedUpdate(
                condition=f"t.{current_col} <> s.{current_col}",
                set={
                    previous_col: f"t.{current_col}",
                    current_col: f"s.{current_col}"
                }
            ) \
            .whenNotMatchedInsert(
                values={
                    self.business_key: f"s.{self.business_key}",
                    current_col: f"s.{current_col}",
                    previous_col: "NULL"
                }
            ) \
            .execute()
