import sys
import json
import boto3
import requests
from datetime import date
from awsglue.utils import getResolvedOptions

# -------- Job arguments --------
args = getResolvedOptions(sys.argv, ["COMPANIES_HOUSE_API_KEY"])

#API_KEY = "b6cba112-1576-4755-a6a8-7868817d9f75"

API_KEY = args["COMPANIES_HOUSE_API_KEY"]

# -------- Config --------
BUCKET = "companieshouse-data-lake-uk"
INGESTION_DATE = date.today().isoformat()
BASE_URL = "https://api.company-information.service.gov.uk"

# -------- Company Numbers --------
COMPANY_NUMBERS = [
    '02557590',  # ARM LIMITED
    '00617987',  # HSBC HOLDINGS PLC
    '02723534',  # ASTRAZENECA PLC
    '04366849',  # SHELL PLC
    'BR025083',  # LINDE
    '00719885',  # RIO TINTO PLC
    '00041424',  # UNILEVER PLC
    '07524813',  # ROLLS-ROYCE HOLDINGS PLC
    '03407696',  # BRITISH AMERICAN TOBACCO P.L.C.
    '11299879',  # ARM HOLDINGS PLC
    '03888792',  # GSK PLC
    '00102498',  # BP P.L.C.
    '00048839',  # BARCLAYS PLC
    'SC095000',  # LLOYDS BANKING GROUP PLC
    '04031152',  # NATIONAL GRID PLC
    '01470151',  # BAE SYSTEMS PLC
    '07876075',  # AON GLOBAL LIMITED
    'SC045551',  # NATWEST GROUP PLC
    '00077536',  # RELX PLC
    '00966425',  # STANDARD CHARTERED PLC
    '05369106'
]

# -------- AWS Clients --------
s3 = boto3.client("s3")
company_info = ["filing-history", "officers"]
# -------- Functions --------
def get_company_info(company_number, company_info):
    url = f"{BASE_URL}/company/{company_number}/{company_info}"
    response = requests.get(url, auth=(API_KEY, ""))
    response.raise_for_status()
    return response.json()

def get_company_overview(company_number):
  url = f"{BASE_URL}/company/{company_number}"
  response = requests.get(url, auth=(API_KEY, ""))
  response.raise_for_status()
  return response.json()

def write_raw_json(data, company_number, dataset_name):
    key = (
        f"raw/companies_house/"
        f"ingestion_date={INGESTION_DATE}/"
        f"company_number={company_number}/"
        f"{dataset_name}.json"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

# -------- Main Execution --------
def main():
    for company in COMPANY_NUMBERS:
        try:
            print(f"Processing {company}...")
            overview_data = get_company_overview(company)
            write_raw_json(overview_data, company, "overview")

            for info in company_info:
              print(f"Processing: {info} for {company}")
              data = get_company_info(company, info)
              write_raw_json(data, company, info)
              print(f"Success: {info} for {company}")
             
            print(f"Success: {company}")
            
        except Exception as e:
            print(f"Failed for {company}: {str(e)}")
      

if __name__ == "__main__":
    main()
