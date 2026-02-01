"""
Pure PySpark pipeline orchestrator (platform-agnostic)
"""

from utils.logger import get_logger

from medallion.bronze.ds2b_company_house import run as bronze_company_house
from medallion.bronze.ds2b_yfinance import run as bronze_yfinance
from medallion.silver.b2s_company_house import run as silver_company_house
from medallion.silver.b2s_yfinance import run as silver_yfinance
from medallion.gold.s2g import run as gold_layer

logger = get_logger("main_pipeline")

# =========================
# CONFIG PATHS
# ========================
CONFIGS = {
    "bronze_company_house": "medallion/bronze/config_company_house.json",
    "bronze_yfinance": "medallion/bronze/yfinance.json",
    "silver_company_house": "medallion/silver/config_company_house.json",
    "silver_yfinance": "medallion/silver/config_yfinance.json",
    "gold": "medallion/gold/config.json"
}

# =========================
# PIPELINE
# =========================
def run_pipeline():
    logger.info("Starting End-to-End PySpark Pipeline")

    bronze_company_house(CONFIGS["bronze_company_house"])
    bronze_yfinance(CONFIGS["bronze_yfinance"])

    silver_company_house(CONFIGS["silver_company_house"])
    silver_yfinance(CONFIGS["silver_yfinance"])

    gold_layer(CONFIGS["gold"])

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    run_pipeline()