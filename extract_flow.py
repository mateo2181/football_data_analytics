from datetime import datetime, timedelta
from config import AWS_S3_BUCKET
from utils import read_and_upload_to_s3
from load_flow import leagues, years
from prefect import task, Flow, Parameter
from prefect.executors import DaskExecutor

executor = DaskExecutor()

# def format_url(coin) -> str:
#     url = "https://production.api.coindesk.com/v2/price/values/"
#     start_time = (datetime.now() - timedelta(days=1)).isoformat(timespec="minutes")
#     end_time = datetime.now().isoformat(timespec="minutes")
#     params = f"?start_date={start_time}&end_date={end_time}&ohlc=false"
#     return url + coin + params

# @task(max_retries=3, retry_delay=timedelta(seconds=10))
# def get_data(coin: str="DOGE"):
#     prices = re.get(format_url(coin)).json()["data"]["entries"]
#     data = pd.DataFrame(prices, columns=["time", "price"])
#     return data

# @task
# def detect_dip(data, threshold = 10):
#     peak = data['price'].max()
#     bottom = data['price'].min()
#     dip = 100 - (bottom/peak) * 100

#     if dip > threshold:
#         return True
#     else:
#         return False

# @task
# def send_to_slack(message: str):
#     r = re.post(
#         os.environ["SLACK_WEBHOOK_URL"],
#         json=message if isinstance(message, dict) else {"text": message},
#     )
#     r.raise_for_status()
#     return

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