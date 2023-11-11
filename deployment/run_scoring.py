# run_scoring.py

import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import requests
import pickle

def load_pickle_model_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return pickle.loads(response.content)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the model: {e}")
        return None
    except pickle.UnpicklingError as e:
        print(f"Error loading the pickle model: {e}")
        return None

def run():
    model_url = 'https://raw.githubusercontent.com/deburky/profit-scoring/main/models/lgb_reg.pkl'
    scoring_model = load_pickle_model_from_url(model_url)

    if scoring_model is not None:
        print("Model loaded successfully!")
    else:
        print("Failed to load the model.")

    # PostgreSQL credentials and settings
    user = "root"
    password = "root"
    database = "postgres_db"
    host = "localhost"
    port = "5432"
    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    schema_name = "dbt_playground"
    table_name = "dbt_transformation"

    # Connect to the PostgreSQL database
    postgres_engine = create_engine(conn_string, connect_args={"options": f"-c search_path={schema_name}"}, isolation_level="AUTOCOMMIT")

    query = f"SELECT * FROM {schema_name}.{table_name}"

    with postgres_engine.connect() as conn:
        query = conn.execute(text(query))
    df = pd.DataFrame(query.fetchall())

    # Extract LoanId and PartyId columns
    id_frame = df[['LoanId', 'PartyId']].copy()

    # # Drop LoanId and PartyId columns from the original DataFrame
    df.drop(columns=['PartyId', 'LoanId'], inplace=True)

    # process categorical inputs
    feats_cat = df.select_dtypes(include=[object]).columns

    for c in feats_cat:
        df[c] = df[c].astype('category')

    # Predict using the scoring model
    id_frame['score'] = scoring_model.predict(df)
    id_frame['score_timestamp'] = pd.Timestamp.now()

    # Calculate the mean of the scores
    mean_score = id_frame['score'].mean()
    print(f"Average ARR: {mean_score:.3%}")

    # Save the DataFrame to a new table in PostgreSQL
    schema_name = 'dbt_playground'
    table_name = 'scoring_outputs'

    with postgres_engine.connect() as conn:

        # Check if the table exists (true or false)
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table_name}'
            )
        """

        start_time = time.time()
        table_exists = conn.execute(text(table_exists_query)).scalar()

        # If the table does not exist, create it
        if not table_exists:
            print("Table not found - creating a new table...")
            id_frame.head(n=0).to_sql(name=table_name, con=conn, if_exists='replace', index=False)
            print("Table created \U0001f44d")

        print("Writing original data to Postgres...")
        id_frame.to_sql(name=table_name, con=conn, if_exists='append', index=False)

        print("Completed successfully \u2713")
        print(f"Time: {time.time() - start_time:.3f}s")

if __name__ == "__main__":
    run()
