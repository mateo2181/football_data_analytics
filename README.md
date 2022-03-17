# Football Data Analytics

Final project data enginerring zoomcamp: https://github.com/DataTalksClub/data-engineering-zoomcamp

## Objective
Process files with football transfers data between 1999 and 2020.

## Setup
1. pip install -r requirements.txt
2. Paste google_credentials.json file in the root.
3. Run cp .env.example .env to create .env file
4. Set variables in .env file.
2. run docker-compose up
3. prefect server create-tenant -n default
4. prefect create project ELT 
5. python register_flows.py
6. run prefect agent local start --label docker