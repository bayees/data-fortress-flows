from importlib.resources import path
import pandas as pd
import requests
from generic_tasks import write_raw
from prefect import flow, task
import os
from dotenv import load_dotenv
import gspread
from gspread import Worksheet
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

load_dotenv()

# define the scope
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

@task
def extract_sheets() -> list[Worksheet]:
    # add credentials to the account
    credentials = ServiceAccountCredentials.from_json_keyfile_name('data-fortress-409619-7311068e688c.json', scope)

    # authorize the clientsheet 
    client = gspread.authorize(credentials)

    # get the instance of the Spreadsheet
    sheet = client.open('Family budget')
    
    return sheet.worksheets()


@task
def extract_all_records(sheet: list[Worksheet]) -> list[dict]:
    dfs = []

    for worksheet in sheet:
        sheeet_records_df = pd.json_normalize(worksheet.get_all_records())
        sheeet_records_df = sheeet_records_df[sheeet_records_df['Year'] == int(worksheet.title)]
        dfs.append(sheeet_records_df)

    all_records_df = pd.concat(dfs).reset_index(drop=True)

    return all_records_df

@flow
def extract__google_sheets():
    sheets = extract_sheets()
    all_records = extract_all_records(sheets)
    write_raw(all_records, "google_sheets__budget/google_sheets__budget")

if __name__ == "__main__":
    extract__google_sheets()
