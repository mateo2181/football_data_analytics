import os
import pandas as pd
from prefect import task, prefect
from google.cloud.bigquery import SourceFormat, LoadJobConfig
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery.table import Table
from clients import gcs, gbq, LOGGER
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

def transform(df:pd.DataFrame) -> pd.DataFrame:
    df["club_name"] = df["club_name"]
    df["player_name"] = df["player_name"]
    df["age"] = df['age'].astype('Int64')
    df["position"] = df["position"]
    df["club_involved_name"] = df["club_involved_name"]
    df["fee"] = df["fee"]
    df["transfer_movement"] = df["transfer_movement"]
    df["transfer_period"] = df["transfer_period"]
    df["fee_cleaned"] = df["fee_cleaned"]
    df["league_name"] = df["league_name"]
    df["year"] = df["year"]
    df["season"] = df["season"]
    return df

def read_csv_from_url(url: str) -> pd.DataFrame:
    df = pd.read_csv(url)
    return df

def upload_csv_to_s3(bucket:str, df: pd.DataFrame, key: str):
    logger = prefect.context.get("logger")
    try:
        df.to_csv(
            f"s3://{bucket}/{key}",
            index=False,
            storage_options={
                "key": AWS_ACCESS_KEY_ID,
                "secret": AWS_SECRET_ACCESS_KEY
            },
        )
        return f"s3://{bucket}/{key}"
    except Exception as e:
        logger.error(e)
        return False

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    df = pd.read_csv(
        f"s3://{bucket}/{key}",
        storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret": AWS_SECRET_ACCESS_KEY
        },
    )
    return df

def upload_csv_to_gcs(bucket: str, df: pd.DataFrame, key: str):
    logger = prefect.context.get("logger")
    try:
        df.to_csv(f"gcs://{bucket}/{key}", index=False)
        return True
    except Exception as e:
        logger.error(e)
        return False

@task
def read_and_upload_to_s3(bucket: str, url: str):
    logger = prefect.context.get("logger")
    logger.info(f"Starting job: reading CSV file from {url}.")
    # LOGGER.info(f"Starting job: reading CSV file from {url}.")
    key = url.split('/')[-2:]
    key = '/'.join(key)
    df = read_csv_from_url(url)
    res = upload_csv_to_s3(bucket, df, key)
    if(res):
        logger.info(f"Finished job: CSV file saved in {res}.")
    return res

@task
def read_from_s3_and_upload_to_gcs(AWS_S3_BUCKET, key:str, folderGCS:str, GCS_BUCKET):
    logger = prefect.context.get("logger")
    fileName = key.replace("/", "_")
    logger.info(f"Starting job: uploading csv {fileName}.")
    df = read_csv_from_s3(AWS_S3_BUCKET, key)
    df = transform(df) # Transform column types Dataframe
    upload_csv_to_gcs(GCS_BUCKET, df, f"{folderGCS}/{fileName}")
    logger.info(f"Finished job: uploaded csv {fileName}.")
    return True

@task
def gcs_csv_to_bq_table(gcs_bucket: str, full_table_id: str, remote_csv_path: str) -> Table:
    """
    Insert CSV from Google Storage to BigQuery Table.

    :param full_table_id: Full ID of a Google BigQuery table.
    :type full_table_id: str
    :param remote_csv_path: Path to uploaded CSV.
    :type remote_csv_path: str
    :returns: str
    """
    logger = prefect.context.get("logger")
    try:
        gcs_csv_uri = f"gs://{gcs_bucket}/{remote_csv_path}"
        job_config = LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            source_format=SourceFormat.CSV,
            write_disposition="WRITE_TRUNCATE"
        )
        load_job = gbq.load_table_from_uri(source_uris=gcs_csv_uri, destination=full_table_id, job_config=job_config)
        logger.info(f"Starting job {load_job.job_id}.")
        logger.success(load_job.result())  # Waits for table load to complete.
        return gbq.get_table(full_table_id)
    except BadRequest as e:
        logger.error(f"Invalid GCP request when creating table `{full_table_id}`: {e}")
    except Exception as e:
        logger.error(f"Unexpected error when creating table `{full_table_id}`: {e}")
