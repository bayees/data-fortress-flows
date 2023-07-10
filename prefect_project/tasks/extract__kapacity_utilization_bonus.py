from minio import Minio
import pandas as pd
import os
from dotenv import load_dotenv
import pandas as pd
import os
import datetime
from prefect import flow, task
from pathlib import Path
import pytz
from .generic_tasks import write_raw

load_dotenv()

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
    
schema_versions = [
    { 'schema': 'v2', 'reference_column_index': 2 },
    { 'schema': 'v3', 'reference_column_index': 1 },
]

@task
def get_lastest_object_modified_watermark() -> datetime.datetime:
    # get max last_modified date from all objects in raw
    last_modified_dates = [o.last_modified for o in client.list_objects('raw', prefix='kapacity_bonus__hours_overview', recursive=True)]
    watermark = max(last_modified_dates) if len(last_modified_dates) > 0 else datetime.datetime(1970, 1, 1, tzinfo = pytz.UTC)

    return watermark

@task
def retrieve_minio_objects(path: str, watermark: datetime.datetime):
    filtered_object = [
        {
            "name": o.object_name,
            "stats": client.stat_object("kapacity-bonus", o.object_name),
            "data": client.get_object("kapacity-bonus", o.object_name).data
        }
        for o in client.list_objects("kapacity-bonus", prefix=path, recursive=True) if o.last_modified > watermark
    ]
    return filtered_object

@task
def parse_object(minio_object, schema_version):
    df = pd.read_excel(minio_object["data"])
    modified_at = minio_object["stats"].last_modified
    object_name = Path(minio_object['name']).stem

    df_dict = {}

    start_index_overview = df.index[df.iloc[:,schema_version["reference_column_index"]].astype(str).str.contains("Christian Bay")].tolist()[0]
    start_index_consultancy_details = df.index[df.iloc[:,schema_version["reference_column_index"]].astype(str).str.contains("Details Overview - Consultancy Hours")].tolist()[0]
    start_index_presales_details = df.index[df.iloc[:,schema_version["reference_column_index"]].astype(str).str.contains("Details Overview - Presales Hours")].tolist()[0]
    start_index_internal_development_details = df.index[df.iloc[:,schema_version["reference_column_index"]].astype(str).str.contains("Details Overview - Internal Development Hours",)].tolist()[0]
    start_index_vacation_details = df.index[df.iloc[:,schema_version["reference_column_index"]].astype(str).str.contains("Details Overview - Vacation Hours")].tolist()[0]
    try:
        start_index_internal_management = df.index[df.iloc[:,schema_version["reference_column_index"]].astype(str).str.contains("Details Overview - Internal Management Hours")].tolist()[0]
    except IndexError:
        start_index_internal_management = None

    df_overview = df.iloc[start_index_overview + 1:start_index_consultancy_details - 1]
    df_overview = df_overview.dropna(axis=1, how='all')
    df_overview.columns = ['key', 'value', 'description']
    df_overview['object_name'] = object_name
    df_overview['value'] = df_overview['value'].fillna(0)
    df_overview = df_overview.dropna(subset=["key"])
    df_overview = df_overview.pivot(index='object_name', columns='key', values='value').reset_index()
    df_overview = df_overview.rename(columns = {"Internal Development Hours":"Internal Development Hours (util)"})
    df_overview['object_modified_at'] = modified_at

    df_dict[f'kapacity_bonus__hours_overview/{object_name}'] = df_overview

    df_consultancy = df.iloc[start_index_consultancy_details + 2:start_index_presales_details - 2]
    df_consultancy.columns = df.iloc[start_index_consultancy_details + 1]
    df_consultancy = df_consultancy.dropna(axis=1, how='all')
    df_consultancy['object_name'] = object_name

    if not df_consultancy.empty:
        df_dict[f'kapacity_bonus__consultancy_hours_detail/{object_name}'] = df_consultancy
    
    df_presales = df.iloc[start_index_presales_details + 2:start_index_internal_development_details - 2]
    df_presales.columns = df.iloc[start_index_presales_details + 1]
    df_presales = df_presales.dropna(axis=1, how='all')
    df_presales['object_name'] = object_name

    if not df_presales.empty:
        df_dict[f'kapacity_bonus__presales_hours_detail/{object_name}'] = df_presales

    df_internal_development = df.iloc[start_index_internal_development_details + 2:start_index_vacation_details - 2]
    df_internal_development.columns = df.iloc[start_index_internal_development_details + 1]
    df_internal_development = df_internal_development.dropna(axis=1, how='all')
    df_internal_development = df_internal_development.rename(columns = {"Internal Dev. Hours":"Hours"})
    df_internal_development['object_name'] = object_name

    if not df_internal_development.empty:
        df_dict[f'kapacity_bonus__internal_development_hours_detail/{object_name}'] = df_internal_development

    # if there is no internal management hours, then the index will be out of range
    if start_index_internal_management:
        df_vacation = df.iloc[start_index_vacation_details + 2:start_index_internal_management - 2]
        df_vacation.columns = df.iloc[start_index_vacation_details + 1]
        df_vacation = df_vacation.dropna(axis=1, how='all')
        df_vacation['object_name'] = object_name
        
        if not df_vacation.empty:
            df_dict[f'kapacity_bonus__vacation_hours_detail/{object_name}'] = df_vacation

        df_internal_management = df.iloc[start_index_internal_management + 2:-1]
        df_internal_management.columns = df.iloc[start_index_internal_management + 1]
        df_internal_management = df_internal_management.dropna(axis=1, how='all')
        df_internal_management['object_name'] = object_name

        if not df_internal_management.empty:
            df_dict[f'kapacity_bonus__internal_management_hours_detail/{object_name}'] = df_internal_management

    else:
        df_vacation = df.iloc[start_index_vacation_details + 2:-1]
        df_vacation.columns = df.iloc[start_index_vacation_details + 1]
        df_vacation = df_vacation.dropna(axis=1, how='all')
        df_vacation['object_name'] = object_name
        
        if not df_vacation.empty:
            df_dict[f'kapacity_bonus__vacation_hours_detail/{object_name}'] = df_vacation

    return df_dict

@flow
def extract__kapacity_utilization_bonus():
    watermark = get_lastest_object_modified_watermark()

    for schema_version in schema_versions:
        path = f'/utilization/{schema_version["schema"]}'

        minio_objects = retrieve_minio_objects(path, watermark)
        
        for object in minio_objects:

            df_dicts = parse_object(object, schema_version)
            for key, value in df_dicts.items():
                write_raw(value, key)

if __name__ == "__main__":
    extract__kapacity_utilization_bonus()
    


