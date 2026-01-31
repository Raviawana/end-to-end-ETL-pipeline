import sys
import sys
import json
import boto3
import requests
from datetime import date
from awsglue.utils import getResolvedOptions

# -------- Job arguments --------
args = getResolvedOptions(sys.argv, ["COMPANIES_HOUSE_API_KEY"])
API_KEY = args["COMPANIES_HOUSE_API_KEY"]

# -------- Config --------
BUCKET = "companieshouse-data-lake-uk"
INGESTION_DATE = date.today().isoformat()
BASE_URL = "https://api.company-information.service.gov.uk"

# Example: start with ONE company only
COMPANY_NUMBER = "02557590"  # ARM LIMITED

# -------- Clients --------
s3 = boto3.client("s3")

def get_company_overview(company_number):
    url = f"{BASE_URL}/company/{company_number}"
    response = requests.get(url, auth=(API_KEY, ""))
    response.raise_for_status()
    return response.json()

def write_raw_json(data, company_number):
    key = (
        f"raw/companies_house/ingestion_date={INGESTION_DATE}/"
        f"company_overview/company_number={company_number}.json"
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data)
    )

def main():
    data = get_company_overview(COMPANY_NUMBER)
    write_raw_json(data, COMPANY_NUMBER)
    print(f"Company overview ingested for {COMPANY_NUMBER}")

if __name__ == "__main__":
    main()