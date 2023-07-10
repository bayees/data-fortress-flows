from importlib.resources import path
import pandas as pd
import requests
import workalendar.europe as workalendar
from .generic_tasks import write_raw
from prefect import flow, task
import os
from dotenv import load_dotenv

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_BASE_URL = os.getenv("NOTION_BASE_URL")

NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

@task
def extract_budget() -> str:
    url = f"{NOTION_BASE_URL}/{NOTION_DATABASE_ID}/query"
    reponse = requests.post(url, headers={
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2021-08-16"
    }).json()

    return reponse

@task
def transform_budget(response: str) -> pd.DataFrame:
    df = pd.json_normalize(response['results'])
    return df


@flow
def extract__notion():
    budget_extract = extract_budget()
    budget_transformed = transform_budget(budget_extract)
    write_raw(budget_transformed, "notion__budget/notion__budget")

if __name__ == "__main__":
    extract__notion()
