# Data Fortress

This is the code for my personal data warehouse.

Considered sources that is going to be implemented is:
 - Spiir
 - Notion


Add dbt relation:
https://medium.com/slateco-blog/prefect-orchestrating-dbt-10f3ca0baea9
https://www.prefect.io/guide/blog/flow-of-flows-orchestrating-elt-with-prefect-and-dbt/

prefect deployment build prefect_project.main_controller:main_run --name "daily" --storage-block github/github-block -q docker-queue

prefect deployment apply main_run-deployment.yaml


docker compose --profile server --profile agent up -d


pip install pandas workalendar dbt-duckdb dbt-core duckdb minio pyarrow python-dotenv requests pytz prefect_dbt s3fs openpyxl pydriller