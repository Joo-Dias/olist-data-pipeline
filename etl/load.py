import os
import pandas as pd
from sqlalchemy import create_engine

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed")

def load_postgres():
    engine = create_engine(
        "postgresql+psycopg2://olist_user:olist_pass@localhost:5434/olist_dw"
    )

    tables = {
        "dim_customers": "dim_customers.csv",
        "dim_products": "dim_products.csv",
        "dim_time": "dim_time.csv",
        "fact_sales": "fact_sales.csv"
    }

    for table, file in tables.items():
        path = os.path.join(PROCESSED_PATH, file)
        df = pd.read_csv(path)
        df.to_sql(table, engine, if_exists="append", index=False)
        print(f"{table} carregada ({df.shape[0]} linhas)")

