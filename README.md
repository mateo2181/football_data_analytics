# Football Data Analytics

Final project data enginerring zoomcamp: https://github.com/DataTalksClub/data-engineering-zoomcamp

## Objective
Process files with football transfers data between 1999 and 2020.

## Sources
The origial repository with csv files: https://github.com/ewenme/transfers 

## Prerequisites
- Installed locally:
    - Python 3
    - Docker with docker-compose
- A project in Google Cloud Platform with a GCS Bucket and a warehouse in Bigquery.
- S3 bucket created in AWS. 

## Setup
1. From IAM in GCP (https://console.cloud.google.com/iam-admin/serviceaccounts) create a new service account, download the credentials file, rename it to "google_credentials.json" and save the file in the root.
2. Run `$ cp .env.example .env` to create .env file
3. Set variables in .env file.
4. Create a python environment (ex: with Anaconda run `$ conda create --name myenv` and activate it with `$ conda activate myenv` ) 
4. Run `$ pip install -r requirements.txt`
2. Run `$ docker-compose up`
3. Run `$ prefect server create-tenant -n default`
4. Run `$ prefect create project ELT`
5. Run `$ python register_flows.py`
6. Run `$ prefect agent local start --label docker`