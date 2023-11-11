# fetch_and_write_data.py

import pandas as pd
import requests
import io
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import warnings
warnings.filterwarnings("ignore")

def fetch_data():
    print("Fetching external data...")
    zf = requests.get("https://www.bondora.com/marketing/media/LoanData.zip")
    df = pd.read_csv(io.BytesIO(zf.content), compression='zip', header=0, sep=',', quotechar='"')
    print("Fetching external data - finished \U0001f44d")
    return df

def write_to_postgres(df, schema_name, table_name, conn_string):
    postgres_engine = create_engine(conn_string, connect_args={"options": f"-c search_path={schema_name}"}, isolation_level="AUTOCOMMIT")

    with postgres_engine.connect() as conn:
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table_name}'
            )
        """

        table_exists = conn.execute(text(table_exists_query)).scalar()

        if not table_exists:
            print("Table not found - creating a new table...")
            df.head(n=0).to_sql(name=table_name, con=conn, if_exists='replace', index=False)
            print("Table created \U0001f44d")

        print("Writing original data to Postgres...")
        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
        print("Completed successfully \u2713")

def run():
    schema_name = "dbt_playground"
    table_name = "bondora_loan_dataset"

    # PostgreSQL credentials and settings
    user = "root"
    password = "root"
    database = "postgres_db"
    host = "localhost"
    port = "5432"
    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    df = fetch_data()
    write_to_postgres(df, schema_name, table_name, conn_string)
