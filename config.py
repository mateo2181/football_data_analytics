from os import getenv, path
from dotenv import load_dotenv
load_dotenv()

AWS_S3_BUCKET = getenv("AWS_S3_BUCKET")
AWS_ACCESS_KEY_ID = getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = getenv("AWS_SECRET_ACCESS_KEY")
GCS_BUCKET = getenv("GCP_GCS_BUCKET")
GCP_BQ_PROJECT = getenv("GCP_BQ_PROJECT")
GCP_BQ_DATASET = getenv("GCP_BQ_DATASET")
GCP_BQ_TABLE_ID = getenv("GCP_BQ_TABLE_ID")
GCP_BQ_LOCATION = getenv("GCP_BQ_LOCATION")

GITHUB_ACCESS_TOKEN = getenv("GITHUB_ACCESS_TOKEN")

GCP_DBT_DATASET = getenv("GCP_DBT_DATASET")
GCP_DBT_DATASET_PROD = getenv("GCP_DBT_DATASET_PROD")
GCP_DBT_KEYFILE = getenv("GCP_DBT_KEYFILE")
GCP_DBT_METHOD = getenv("GCP_DBT_METHOD")