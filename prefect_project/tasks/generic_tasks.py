import os
from prefect import task
import duckdb
from duckdb import IOException
import pandas as pd
from dotenv import load_dotenv
import pyarrow as pa
from pyarrow import fs, parquet
from minio import Minio
import datetime

load_dotenv()

MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_SSL = os.environ.get("MINIO_SSL", "False") == "True"
MINIO_REGION = os.environ.get("MINIO_REGION")

def config(con):
    con.sql("INSTALL httpfs")
    con.sql("LOAD httpfs")
    con.sql(f"SET s3_region='{MINIO_REGION}';")
    con.sql(f"SET s3_endpoint='{MINIO_HOST}';")
    con.sql(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.sql(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.sql(f"SET s3_use_ssl={MINIO_SSL};")
    con.sql("SET s3_url_style='path';")

@task
def write_raw(df: pd.DataFrame, path: str, columns: list = []) -> None:
    # Adding empty dataframe to enforce columns
    empty = pd.DataFrame(columns=columns)

    df = pd.concat([empty, df], ignore_index=True)
    with duckdb.connect() as con:
        config(con)
        con.sql(f"COPY df TO 's3://raw/{path}.parquet';")

@task
def read_curated(file: str, columns:list=['*'], filter:str = '') -> pd.DataFrame:
    # Adding empty dataframe to enforce columns
    with duckdb.connect() as con:
        config(con)
        df = con.sql(f"SELECT {', '.join(columns)} FROM read_parquet('s3://{file}') {filter};").df()
        return df
    
@task
def read_watermark(column: str, folder: str) -> datetime.datetime:
    # Adding empty dataframe to enforce columns
    with duckdb.connect() as con:
        config(con)
        try:
            return con.sql(f"SELECT MAX({column}) FROM read_parquet('s3://{folder}');").df().iloc[0,0]
        except IOException:
            return None
