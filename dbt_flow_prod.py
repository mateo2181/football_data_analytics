from config import GCP_DBT_DATASET_PROD, GCP_BQ_PROJECT, GCP_BQ_LOCATION, GCP_DBT_METHOD, GCP_DBT_KEYFILE
import prefect
from prefect import Flow
from prefect.tasks.dbt.dbt import DbtShellTask
from dbt_flow import pull_dbt_repo
from dbt_flow import print_dbt_output
from prefect.storage import GitHub
from prefect.run_configs import LocalRun

STORAGE = GitHub(
    repo="mateo2181/football_data_analytics",
    path=f"dbt_flow_prod.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)

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

with Flow("DBT PRODUCTION: clone_dbt_and_run_models",
    storage=STORAGE,
    run_config=LocalRun(labels=["dev"])
) as flow:
    pull_repo = pull_dbt_repo()
    dbt_run = dbt(command="dbt build -t prod", upstream_tasks=[pull_repo])
    dbt_run_out = print_dbt_output(dbt_run, task_args={"name": "DBT PROD Run Output"})

if __name__ == "__main__":
    state =flow.run()