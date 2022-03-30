import pygit2
import subprocess
from config import GCP_DBT_DATASET, GCP_BQ_PROJECT, GCP_BQ_LOCATION, GCP_DBT_METHOD, GCP_DBT_KEYFILE
import prefect
from prefect import task, Flow
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.triggers import all_finished
from prefect.storage import GitHub
from prefect.run_configs import LocalRun

STORAGE = GitHub(
    repo="mateo2181/football_data_analytics",
    path=f"dbt_flow.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)

@task
def pull_dbt_repo():
    logger = prefect.context.get("logger")
    logger.info(f"Cloning DBT repository from Github...")
    subprocess.run(["rm", "-rf", "dbt"]) # Delete folder on run
    # git_token = GITHUB_ACCESS_TOKEN
    dbt_repo_name = "football_data_dbt"
    # dbt_repo = (f"https://{git_token}:x-oauth-basic@github.com/mateo2181/{dbt_repo_name}")
    dbt_repo = (f"https://github.com/mateo2181/{dbt_repo_name}")
    pygit2.clone_repository(dbt_repo, "dbt")
    logger.info(f"DTB repository cloned successfully.")


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

@task(trigger=all_finished)
def print_dbt_output(output):
    logger = prefect.context.get("logger")
    for line in output:
        logger.info(line)


with Flow("clone_dbt_and_run_models",
    storage=STORAGE,
    run_config=LocalRun(labels=["dev"])
) as flow:
    pull_repo = pull_dbt_repo()
    dbt_run = dbt(command="dbt build", upstream_tasks=[pull_repo])
    dbt_run_out = print_dbt_output(dbt_run, task_args={"name": "DBT Run Output"})

if __name__ == "__main__":
    state =flow.run()
    # flow.visualize()