import encodings
import zipfile
import pandas as pd
import json
import glob
import os
import datetime
from prefect import flow, task
from .generic_tasks import write_raw
from minio import Minio
import pytz
import shutil

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

@task(name="get_lastest_object_modified_watermark_storebox")
def get_lastest_object_modified_watermark() -> datetime.datetime:
    # get max last_modified date from all objects in raw
    last_modified_dates = [o.last_modified for o in client.list_objects('raw', prefix='storebox__receipts', recursive=True)]
    watermark = max(last_modified_dates) if len(last_modified_dates) > 0 else datetime.datetime(1970, 1, 1, tzinfo = pytz.UTC)

    return watermark

@task(name="retrieve_minio_objects_storebox")
def retrieve_minio_objects(watermark: datetime.datetime):
    path = os.path.join(os.getcwd(), 'tmp/')
    bucket_name = "landing"
    for item in client.list_objects(bucket_name, prefix='storebox-dump', recursive=True):
        if item.last_modified > watermark:
            client.fget_object(bucket_name, item.object_name, path + item.object_name)

    files = glob.glob(os.path.join(path, "**/*.zip"))    
    print(files)
    return files

@task
def unzip__file(file):
    fh = open(file, 'rb')
    outpath = os.path.dirname(file) + '/' + os.path.basename(file).split('.')[0]
    z = zipfile.ZipFile(fh)
    for info in z.infolist():
        if info.filename.endswith('.json'):
            z.extract(info, outpath)
    fh.close()
    return outpath

@task
def parse_file(file):
    with open(file, 'rb') as user_file:
        file_contents = json.load(user_file)
        data = pd.json_normalize(file_contents)
    return data

@task
def clean_tmp_folder(dependencies:list):
    shutil.rmtree(os.path.join(os.getcwd(), 'tmp/'))

@flow
def extract__storebox():
    watermark = get_lastest_object_modified_watermark()
    
    objects = retrieve_minio_objects(watermark)
    storebox__cards = None
    storebox__receipts = None
    storebox__user = None

    for object in objects:
        outpath = unzip__file(object)
       
        cards_file = glob.glob(outpath+'/*/cards-*.json')
        storebox__cards = write_raw( parse_file(cards_file[0]) , 'storebox__cards/storebox__cards' )
       
        cards_file = glob.glob(outpath+'/*/receipts-*.json')
        storebox__receipts = write_raw( parse_file(cards_file[0]) , 'storebox__receipts/storebox__receipts'  )
       
        cards_file = glob.glob(outpath+'/*/user-*.json')
        storebox__user = write_raw( parse_file(cards_file[0]) , 'storebox__user/storebox__user')

    if storebox__cards and storebox__receipts and storebox__user:
        clean_tmp_folder([storebox__cards, storebox__receipts, storebox__user])

if __name__ == "__main__":
    extract__storebox()
