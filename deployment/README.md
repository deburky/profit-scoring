### leveraging dbt and Python for modeling pipelines

To use the `main.py` script, follow these steps:

* Ensure that you have all the required Python scripts (`fetch_and_write_data.py`, `dbt_transform_data.py`, `run_scoring.py`) in the same directory as the `main.py` script

* Open a terminal or command prompt and navigate to the directory where the `main.py` script is located

* Run the `main.py` script using the following command:

```
python3 main.py
```

The `main.py` script will execute the three steps in sequence:

* Fetch and write data: the `fetch_and_write_data.run()` function will be called to fetch external data and store it in the Postgres database

* Transform data with dbt: the `dbt_transform_data.run()` function will be called to perform data transformations using dbt

* Run scoring model: the `run_scoring.run()` function will be called to load the pre-trained scoring model, perform predictions, and save the results to the database

After completing all the steps, the script will print "Entire process completed successfully ✓" along with the total time taken for the entire process.

**Note:** Before running the `main.py` script, make sure you have set up the Postgres database and dbt project as described in [the Medium article](https://medium.com/@dennyemb/leveraging-dbt-and-python-for-predictive-modeling-51367a7c4438). Additionally, ensure that you have installed all the required Python packages mentioned in the scripts:

```
pip install pandas requests scikit-learn lightgbm==3.3.5 psycopg2-binary sqlalchemy dbt-core dbt-postgres
```