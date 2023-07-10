from importlib.resources import path
import pandas as pd
import requests
import datetime
import workalendar.europe as workalendar
from datetime import datetime, timedelta
from tasks.generic_tasks import write_raw
from prefect import flow, task
from dotenv import load_dotenv
import os

load_dotenv()

TODOIST_BASE_URL = os.getenv('TODOIST_BASE_URL')
TODOIST_TOKEN = os.getenv('TODOIST_TOKEN')

@task
def extract_activities() -> list:
    item_url = f"{TODOIST_BASE_URL}/sync/v9/activity/get"

    reponses = []
    i = 0
    limit = 30
    count = limit

    while i <= int(count / limit):
        offset = limit * i
        data = {'limit': limit, 'offset': offset}
        reponse = requests.post(item_url, headers={ "Authorization": f"Bearer {TODOIST_TOKEN}" }, timeout=10, data=data).json()
        reponses.append(reponse)
        count = int(reponse['count'])
        i += 1

    return reponses


@task
def extract_projects() -> dict:
    item_url = f"{TODOIST_BASE_URL}/rest/v2/projects"
    reponse = requests.get(item_url, headers={ "Authorization": f"Bearer {TODOIST_TOKEN}" }, timeout=10).json()
    return reponse

@task
def extract_tasks() -> dict:
    item_url = f"{TODOIST_BASE_URL}/rest/v2/tasks"
    reponse = requests.get(item_url, headers={ "Authorization": f"Bearer {TODOIST_TOKEN}" }, timeout=10).json()
    return reponse


@task
def transform_activities(responses: list) -> pd.DataFrame:
    dfs = []
    for response in responses:
        df = pd.json_normalize(response['events'])
        dfs.append(df)
    df = pd.concat(dfs)
    df.columns = df.columns.str.replace(r"[().]", "_", regex=True)
    return df


@task
def transform_entity(response: dict) -> pd.DataFrame:
    df = pd.json_normalize(response)
    df.columns = df.columns.str.replace(r"[().]", "_", regex=True)
    return df

@flow
def extract__todoist():
    activities_extract = extract_activities()
    activities_transformed = transform_activities(activities_extract)
    activities_success = write_raw(activities_transformed, 'todoist__activities/todoist__activities', ['extra_data_description', 'extra_data_last_description'])

    projects_extract = extract_projects()
    projects_transformed = transform_entity(projects_extract)
    projects_success = write_raw(projects_transformed, 'todoist__projects/todoist__projects')

    tasks_extract = extract_tasks()
    tasks_transformed = transform_entity(tasks_extract)
    tasks_success = write_raw(tasks_transformed, 'todoist__tasks/todoist__tasks')

if __name__ == "__main__":
    extract__todoist()
