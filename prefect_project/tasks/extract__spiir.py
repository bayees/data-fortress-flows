import requests
import pandas as pd
import sqlalchemy as sa
from prefect import flow, task
from .generic_tasks import write_raw
from dotenv import load_dotenv
import os

load_dotenv()

SPIIR_BASE_URL = os.getenv('SPIIR_BASE_URL')
SPIIR_EMAIL = os.getenv('SPIIR_EMAIL')
SPIIR_PASSWORD = os.getenv('SPIIR_PASSWORD')

@task
def extract_postings() -> dict:
    with requests.Session() as s:
        s.post(f'{SPIIR_BASE_URL}/log-ind', data={ 'Email': SPIIR_EMAIL, 'Password': SPIIR_PASSWORD })
        response = s.get(f'{SPIIR_BASE_URL}/Profile/ExportAllPostingsToJson').json()
        return response

@task
def transform_postings(response: dict) -> pd.DataFrame:
    df = pd.json_normalize(response)
    df.columns = df.columns.str.replace(r"[().]", "_", regex=True)
    return df

@flow
def extract__spiir():
    extract = extract_postings()
    transformed = transform_postings(extract)
    write_raw(transformed, "spiir__postings/spiir__postings")

if __name__ == "__main__":
    extract__spiir()