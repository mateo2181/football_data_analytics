from prefect import Flow
from prefect.tasks.dbt.dbt import DbtShellTask
from clients import LOGGER
from config import GCP_DBT_DATASET_PROD, GCP_BQ_PROJECT, GCP_BQ_LOCATION, GCP_DBT_METHOD, GCP_DBT_KEYFILE
from dbt_flow import pull_dbt_repo

dbt = DbtShellTask(
    return_all=True,
    profile_name="football_dbt",
    environment="prod",
    # profiles_dir="./dbt/football_dbt",
    overwrite_profiles=True,
    log_stdout=True,
    helper_script="cd dbt/football_dbt",
    log_stderr=True,
    dbt_kwargs = {
        "type": "bigquery",
        "priority": 'interactive',
        "threads": 4,
        "timeout_seconds": 300,
        "project": GCP_BQ_PROJECT,
        "dataset": GCP_DBT_DATASET_PROD,
        "location": GCP_BQ_LOCATION,
        "keyfile": GCP_DBT_KEYFILE,
        "method": GCP_DBT_METHOD
    }
)

with Flow("DBT PRODUCTION: clone_dbt_and_run_models") as flow:
    pull_repo = pull_dbt_repo()
    res = dbt(
        command="dbt build -t prod",
        upstream_tasks=[pull_repo],
    )

if __name__ == "__main__":
    state =flow.run()