from datetime import datetime, timedelta
from config import AWS_S3_BUCKET
from utils import read_and_upload_to_s3
from load_flow import leagues, years
from prefect import task, Flow, Parameter
from prefect.executors import DaskExecutor

executor = DaskExecutor()

url = 'https://raw.githubusercontent.com/ewenme/transfers/master/data'

@task
def leaguesUrl(years, leagues) -> tuple[str]: 
    list = []
    i = 0
    for year in years:
        for league in leagues:
            githubLink = f"{url}/{str(year)}/{league}.csv"
            list.insert(i, githubLink)
            i = i + 1
    return list

@task
def extract_files_from_gh_and_move_to_s3(githubUrl):
    read_and_upload_to_s3.run(AWS_S3_BUCKET, githubUrl)

with Flow("extract_football_leagues_files_s3") as flow:
    urlLeagues = leaguesUrl(years, leagues)
    extractFilesTask = extract_files_from_gh_and_move_to_s3.map(urlLeagues)

if __name__ == "__main__":
    state =flow.run(executor=executor)