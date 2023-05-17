from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow
def dbt_build() -> str:
    result = DbtCoreOperation(
        commands=["dbt build"],
        project_dir="dbt_project",
    ).run()
    return result

dbt_build()