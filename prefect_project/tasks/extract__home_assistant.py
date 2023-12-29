import requests
import json
from importlib.resources import path
import pandas as pd
import datetime
import workalendar.europe as workalendar
from datetime import datetime, timedelta
from prefect import flow, task
from generic_tasks import write_raw
import os
from dotenv import load_dotenv
from minio import Minio
import re
from datetime import datetime, timedelta
from dateutil import parser
from websockets.sync.client import connect
from minio import Minio
import io
import json

load_dotenv()

HOME_ASSISTANT_BASE_URL = os.getenv('HOME_ASSISTANT_BASE_URL')
HOME_ASSISTANT_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {os.getenv('HOME_ASSISTANT_TOKEN')}",
}


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

@task
def get_missing_dates() -> list:
    # Retrieve all dates that is missing as object in minio
    object_dates = {parser.parse(o.object_name, fuzzy=True).date() for o in client.list_objects('raw', prefix='home_assistant__states', recursive=True)}
    
    base = datetime.today()
    date_list = {(base - timedelta(days=x) - timedelta(days=1)).date() for x in range(30)}
    return date_list.difference(object_dates)

@task
def extract_states(entity: str, missing_dates: list):
    responses = {}

    for date in missing_dates:
        item_url = f"{HOME_ASSISTANT_BASE_URL}/api/history/period/{date}?filter_entity_id={entity}"
        response = requests.get(item_url, headers=HOME_ASSISTANT_HEADERS, timeout=10).json()
        responses.update({date: response})

    return responses

@task
def transform_states(responses: list) -> pd.DataFrame:
    dfs = []
    for response in responses:
        df = pd.json_normalize(response, max_level=0)
        dfs.append(df)
    df = pd.concat(dfs)
    
    # Filter where attributes is different from {}
    df = df[df['attributes'] != {}]

    df.columns = df.columns.str.replace(r"[().]", "_", regex=True)
    return df

@task
def extract_zones():
    with connect(f"{os.getenv('HOME_ASSISTANT_BASE_URL').replace('http', 'ws')}/api/websocket") as websocket:
        auth = {
            "type": "auth",
            "access_token": os.getenv('HOME_ASSISTANT_TOKEN')
        }

        websocket.send(json.dumps(auth))
        message = json.loads(websocket.recv())

        while message['type'] != "auth_ok":
            message = json.loads(websocket.recv())

        websocket.send(json.dumps({ 'type': 'zone/list', 'id': 4 })) # Found the path by looking af the network traffic in the browser
        message = json.loads(websocket.recv())
        
        return pd.DataFrame(message['result'])

@task
def write_landing_states(date, states) -> list:
    # Retrieve all dates that is missing as object in minio
    content = json.dumps(states).encode('utf-8')
    
    client.put_object(
            bucket_name='landing',
            object_name=f'home-assistant-states/home_assistant__states_increment_{int(date.strftime("%Y%m%d"))}.json',
            data=io.BytesIO(content),
            length=len(content),
        )


@flow
def extract__home_assistant():
    missing_dates = get_missing_dates()

    states_per_dates = extract_states(
        entity="device_tracker.chriphone", missing_dates=missing_dates
    )

    for date, states in states_per_dates.items():
        write_landing_result = write_landing_states(date, states)
        states_transformed = transform_states(write_landing_result, states)
        locations_success = write_raw(states_transformed, f'home_assistant__states/home_assistant__states_increment_{int(date.strftime("%Y%m%d"))}')

    zones_transformed = extract_zones()
    write_raw(zones_transformed, f'home_assistant__zones/home_assistant__zones')

if __name__ == "__main__":
    extract__home_assistant()

