# dbt_transform_data.py

import os
from dbt.cli.main import dbtRunner

def run():
    # Configure logging and dbt project directory
    dbt_project_dir = "/Users/deburky/Documents/python/dbt/dbt-python/my_dbt_project"
    os.chdir(dbt_project_dir)

    # Initialize dbtRunner
    dbt = dbtRunner()

    # Define the model name to run
    model_name = "dbt_transformation"

    # Create CLI args as a list of strings
    cli_args = ["run", "--select", f"{model_name}"]

    # Run the command with additional options as keyword arguments
    res = dbt.invoke(cli_args, user="root", password="root")

    # Log the results
    for r in res.result:
        print(f"{r.node.name}: {r.status}")
