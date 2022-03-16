from prefect import task, Flow
from prefect.tasks.dbt.dbt import DbtShellTask
import pygit2
import subprocess
from clients import LOGGER
from config import GITHUB_ACCESS_TOKEN, GCP_DBT_DATASET, GCP_BQ_PROJECT, GCP_BQ_LOCATION, GCP_DBT_METHOD, GCP_DBT_KEYFILE
@task
def pull_dbt_repo():
    LOGGER.info(f"Cloning DBT repository from Github...")
    subprocess.run(["rm", "-rf", "dbt"]) # Delete folder on run
    # git_token = GITHUB_ACCESS_TOKEN
    dbt_repo_name = "football_data_dbt"
    # dbt_repo = (f"https://{git_token}:x-oauth-basic@github.com/mateo2181/{dbt_repo_name}")
    dbt_repo = (f"https://github.com/mateo2181/{dbt_repo_name}")
    pygit2.clone_repository(dbt_repo, "dbt")
    LOGGER.success(f"DTB repository cloned successfully.")


dbt = DbtShellTask(
    return_all=True,
    profile_name="football_dbt",
    environment="dev",
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
        "dataset": GCP_DBT_DATASET,
        "location": GCP_BQ_LOCATION,
        "keyfile": GCP_DBT_KEYFILE,
        "method": GCP_DBT_METHOD
    }
)

with Flow("clone_dbt_and_run_models") as flow:
    pull_repo = pull_dbt_repo()
    res = dbt(
        command="dbt build",
        upstream_tasks=[pull_repo],
    )

if __name__ == "__main__":
    state =flow.run()
    # flow.visualize()