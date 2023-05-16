from prefect_dbt.cli import DbtCoreOperation
from prefect import flow
from extract__calendar import extract__calendar
from extract__dawa import extract__dawa
from extract__home_assistant import extract__home_assistant
from extract__kapacity_utilization_bonus import extract__kapacity_utilization_bonus
from extract__spiir import extract__spiir
from extract__storebox import extract__storebox
from extract__todoist import extract__todoist

@flow
def main_run():
    calendar_result = extract__calendar()
    dawa_result = extract__dawa()
    home_assistant_result = extract__home_assistant()
    kapacity_utilization_bonus_result = extract__kapacity_utilization_bonus()
    spiir_result = extract__spiir()
    storebox_result = extract__storebox()
    todoist_result = extract__todoist()

    DbtCoreOperation(
        commands=["dbt build"],
        project_dir="dbt_project",
        wait_for=[calendar_result, dawa_result, home_assistant_result, kapacity_utilization_bonus_result, spiir_result, storebox_result, todoist_result],
    ).run()

if __name__ == "__main__":
    main_run()