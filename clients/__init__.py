"""Instantiate Google BigQuery Client."""
from google.cloud import bigquery as google_bigquery
from google.cloud import storage
from config import GCP_BQ_PROJECT

from .log import LOGGER

gbq = google_bigquery.Client(project=GCP_BQ_PROJECT)

gcs = storage.Client(GCP_BQ_PROJECT)