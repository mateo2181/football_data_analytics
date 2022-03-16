import subprocess

subprocess.run(["prefect", "register", "--project", "ELT", "--path", "extract_flow.py"])
subprocess.run(["prefect", "register", "--project", "ELT", "--path", "load_flow.py"])
subprocess.run(["prefect", "register", "--project", "ELT", "--path", "dbt_flow.py"])
subprocess.run(["prefect", "register", "--project", "ELT", "--path", "elt.py"])
subprocess.run(["prefect", "register", "--project", "ELT", "--path", "dbt_flow_prod.py"])