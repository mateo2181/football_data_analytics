

import requests as re
import pandas as pd

from datetime import datetime, timedelta
import os
from utils import read_from_s3_and_upload_to_gcs, gcs_csv_to_bq_table
from prefect import task, Flow
from config import GCS_BUCKET, GCP_BQ_PROJECT, GCP_BQ_DATASET, GCP_BQ_TABLE_ID, AWS_S3_BUCKET

GCS_FOLDER='transfers'

leagues: list = [
    'dutch_eredivisie',
    'english_premier_league',
    'french_ligue_1',
    'german_bundesliga_1',
    'italian_serie_a',
    'portugese_liga_nos',
    'spanish_primera_division'
]
years: list = list(range(1999,2021,1))
url = 'https://raw.githubusercontent.com/ewenme/transfers/master/data'

@task
def leaguesUrl(years, leagues) -> tuple[str]: 
    list = []
    i = 0
    for year in years:
        for league in leagues:
            list.insert(i, str(year) + '/' + league + '.csv')
            i = i + 1
    return list

@task
def move_files_from_s3_to_gcs(urlLeague):
    read_from_s3_and_upload_to_gcs.run(AWS_S3_BUCKET, urlLeague, GCS_FOLDER, GCS_BUCKET)


with Flow("load_files_gcs_and_tables_bigquery") as flow:
    urlLeagues = leaguesUrl(years, leagues)
    moveFilesTask = move_files_from_s3_to_gcs.map(urlLeagues)

    # gcs_to_bq.run(GCS_BUCKET, GCS_FOLDER, GCP_BQ_DATASET, GCP_BQ_PROJECT)
    full_table_id = f"{GCP_BQ_PROJECT}.{GCP_BQ_DATASET}.{GCP_BQ_TABLE_ID}"
    gcs_csv_to_bq_table(GCS_BUCKET, full_table_id, 'transfers/*.csv', upstream_tasks=[moveFilesTask])


if __name__ == "__main__":
    state =flow.run()
    # flow.visualize()
# print(state.result)