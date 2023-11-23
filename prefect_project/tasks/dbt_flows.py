from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow
def dbt_build() -> str:
    result = DbtCoreOperation(
        commands=["dbt deps", "dbt build"],
        project_dir="dbt_project",
    ).run()
    return result

if __name__ == "__main__":
    dbt_build()