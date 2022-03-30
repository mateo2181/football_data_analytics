from datetime import timedelta, datetime
from prefect import Flow
from prefect.executors import DaskExecutor
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.schedules import IntervalSchedule
from prefect.run_configs import LocalRun
from prefect.storage import GitHub

STORAGE = GitHub(
    repo="mateo2181/football_data_analytics",
    path=f"elt.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)

executor = DaskExecutor()

# Schedule is not used. Could be added to the flow or handle it from the UI.
schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(days=1)
) 

with Flow("Main ELT",
    storage=STORAGE,
    run_config=LocalRun(labels=["dev"]) 
) as flow:
    load_flow =  create_flow_run(flow_name="load_files_gcs_and_tables_bigquery", project_name="ELT")
    wait_for_flow_load = wait_for_flow_run(load_flow, raise_final_state=True)

    dbt_flow =  create_flow_run(flow_name="clone_dbt_and_run_models", project_name="ELT")
    dbt_flow.set_upstream(wait_for_flow_load)


if __name__ == "__main__":
    state =flow.run()
    # flow.visualize()