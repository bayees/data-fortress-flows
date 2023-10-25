from prefect import flow
from prefect_project.tasks.extract__calendar import extract__calendar
from prefect_project.tasks.extract__dawa import extract__dawa
from prefect_project.tasks.extract__home_assistant import extract__home_assistant
from prefect_project.tasks.extract__kapacity_utilization_bonus import extract__kapacity_utilization_bonus
from prefect_project.tasks.extract__spiir import extract__spiir
from prefect_project.tasks.extract__storebox import extract__storebox
from prefect_project.tasks.extract__todoist import extract__todoist
from prefect_project.tasks.extract__notion import extract__notion
from prefect_project.tasks.extract__logseq import extract__logseq
from prefect_project.tasks.dbt_flows import dbt_build

@flow
def main_run():
    calendar_result = extract__calendar()
    dawa_result = extract__dawa()
    home_assistant_result = extract__home_assistant()
    kapacity_utilization_bonus_result = extract__kapacity_utilization_bonus()
    spiir_result = extract__spiir()
    storebox_result = extract__storebox()
    #todoist_result = extract__todoist()
    #logseq_result = extract__logseq()
    notion_result = extract__notion()

    dbt_build(wait_for=[calendar_result, dawa_result, home_assistant_result, kapacity_utilization_bonus_result, spiir_result, storebox_result, notion_result])

if __name__ == "__main__":
    main_run()