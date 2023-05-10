import requests
from prefect import flow, task

@task
def call_api(url):
    response = requests.get(url)
    print(response.status_code)
    return response.json()

@flow
def main_flow(url="https://catfact.ninja/fact"):
    fact_json = call_api(url)
    return fact_json

if __name__ == "main":
    print(main_flow("https://catfact.ninja/fact"))