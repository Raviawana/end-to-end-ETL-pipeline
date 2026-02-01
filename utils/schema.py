from pyspark.sql.types import *

# ==============================
# BRONZE SCHEMAS - Company House
# ==============================

overview_schema = StructType([
    StructField("company_name", StringType()),
    StructField("company_number", StringType()),
    StructField("company_status", StringType()),
    StructField("date_of_creation", StringType()),
    StructField("jurisdiction", StringType()),
    StructField("type", StringType()),
    StructField("etag", StringType()),
    StructField("has_charges", BooleanType()),
    StructField("has_insolvency_history", BooleanType())
])

officers_schema = StructType([
    StructField("items", ArrayType(StructType([
        StructField("name", StringType()),
        StructField("officer_role", StringType()),
        StructField("appointed_on", StringType()),
        StructField("nationality", StringType())
    ])))
])

filing_schema = StructType([
    StructField("items", ArrayType(StructType([
        StructField("date", StringType()),
        StructField("type", StringType()),
        StructField("description", StringType()),
        StructField("category", StringType())
    ])))
])

# =========================
# SCHEMA MAP
# =========================

SCHEMA_MAP = {
    "overview": overview_schema,
    "officers": officers_schema,
    "filing_history": filing_schema
}





YFINANCE_SCHEMA_MAP = {

    # =========================
    # COMPANY PROFILE / DETAILS
    # =========================
    "company_details": StructType([
        StructField("company_name", StringType(), True),
        StructField("company_number", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("long_name", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("country", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("market_cap", LongType(), True),
        StructField("website", StringType(), True),
        StructField("ingestion_date", DateType(), True)
    ]),

    # =========================
    # FUNDAMENTALS (QUARTERLY)
    # =========================
    "fundamentals_data": StructType([
        StructField("company_name", StringType(), True),
        StructField("company_number", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("quarter_end_date", DateType(), True),
        StructField("total_revenue", DoubleType(), True),
        StructField("gross_profit", DoubleType(), True),
        StructField("operating_income", DoubleType(), True),
        StructField("net_income", DoubleType(), True),
        StructField("ebitda", DoubleType(), True),
        StructField("total_assets", DoubleType(), True),
        StructField("total_liabilities", DoubleType(), True),
        StructField("cash", DoubleType(), True),
        StructField("long_term_debt", DoubleType(), True),
        StructField("operating_cash_flow", DoubleType(), True),
        StructField("capital_expenditure", DoubleType(), True),
        StructField("free_cash_flow", DoubleType(), True),
        StructField("ingestion_date", DateType(), True)
    ]),

    # =========================
    # DAILY TRADING DATA
    # =========================
    "trading_data": StructType([
        StructField("company_number", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("date", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("adj_close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("ingestion_date", DateType(), True)
    ])
}