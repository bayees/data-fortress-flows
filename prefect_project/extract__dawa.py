import requests
from importlib.resources import path
import pandas as pd
import workalendar.europe as workalendar
from datetime import datetime, timedelta
from prefect import flow, task
from generic_tasks import write_raw, read_curated
from minio import Minio
import os
from dotenv import load_dotenv
from pyarrow import fs, parquet


load_dotenv()

DAWA_BASE_URL = "https://api.dataforsyningen.dk"

MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_SSL = os.environ.get("MINIO_SSL", "False") == "True"
MINIO_REGION = os.environ.get("MINIO_REGION")

client = Minio(
    endpoint=MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SSL
)


minio = fs.S3FileSystem(
    endpoint_override=MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    scheme='http',
    region=MINIO_REGION)


@task
def check_missing_addresses(df: pd.DataFrame) -> bool:
    return len(df) > 0


@task
def extract_addresses(locations: pd.DataFrame) -> dict:
    responses = []
    for index, location in locations.iterrows():
        longitude_degrees = location['longitude_degrees']
        latitude_degrees = location['latitude_degrees']
        item_url = f"{DAWA_BASE_URL}/adgangsadresser/reverse?x={longitude_degrees}&y={latitude_degrees}"
        response = requests.get(item_url, timeout=10).json()
        responses.append((location, response))
    
    return responses


@task
def transform(responses: list) -> pd.DataFrame:
    dfs = []
    for location, response in responses:
        df = pd.json_normalize(response)
        (longitude_degrees, latitude_degrees) = location
        df['longitude_degrees'] = longitude_degrees
        df['latitude_degrees'] = latitude_degrees
        dfs.append(df)
    df = pd.concat(dfs)
    df.columns = df.columns.str.replace(r"[().]", "_", regex=True)
    return df

@flow
def extract__dawa():
    missing_locations = read_curated('dash/location.parquet', ['longitude_degrees', 'latitude_degrees'], 'where dawa_enriched = 0')

    if check_missing_addresses(missing_locations):
        locations_extract = extract_addresses(locations=missing_locations)
        locations_transformed = transform(locations_extract)
        locations_success = write_raw(locations_transformed, f'dawa__addresses/dawa__addresses_increment__{int(datetime.now().strftime("%Y%m%d"))}')
    
if __name__ == "__main__":
    extract__dawa()

