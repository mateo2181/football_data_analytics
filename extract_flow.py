import datetime
from config import AWS_S3_BUCKET
from utils import read_and_upload_to_s3
from load_flow import leagues, years
from prefect import task, Flow, Parameter, prefect
from prefect.executors import LocalDaskExecutor
from prefect.schedules import clocks, Schedule
from prefect.storage import GitHub
from prefect.run_configs import LocalRun

executor = LocalDaskExecutor()

start_date = datetime.datetime(1999, 1, 1)

clock = clocks.IntervalClock(
    start_date=start_date,
    interval=datetime.timedelta(days=1)
)

schedule = Schedule(
    clocks=[clock],
    filters=[prefect.schedules.filters.between_dates(1,1,1,1)]
)

STORAGE = GitHub(
    repo="mateo2181/football_data_analytics",
    path=f"extract_flow.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)

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

with Flow("extract_football_leagues_files_s3",
          executor=executor,
          storage=STORAGE,
          run_config=LocalRun(labels=["dev"])
) as flow:
    # logger = prefect.context.get("logger")
    # current_time = prefect.context.get("scheduled_start_time")
    # logger.info(f"current_time: {current_time}")
    urlLeagues = leaguesUrl(years, leagues)
    extractFilesTask = extract_files_from_gh_and_move_to_s3.map(urlLeagues)

if __name__ == "__main__":
    # flow.schedule = schedule
    state =flow.run()