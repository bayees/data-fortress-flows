from prefect import flow
from extract__calendar import extract__calendar
from extract__dawa import extract__dawa
from extract__home_assistant import extract__home_assistant
from extract__kapacity_utilization_bonus import extract__kapacity_utilization_bonus
from extract__spiir import extract__spiir
from extract__storebox import extract__storebox
from extract__todoist import extract__todoist

@flow
def main_flow():
    extract__calendar()
    extract__dawa()
    extract__home_assistant()
    extract__kapacity_utilization_bonus()
    extract__spiir()
    extract__storebox()
    extract__todoist()

if __name__ == "__main__":
    main_flow()