import os
import pandas as pd

RAW_PATH = "data/raw"

FILES = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv"
}

def extract():
    dataframes = {}

    for name, file in FILES.items():
        file_path = os.path.join(RAW_PATH, file)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file}")

        print(f"Lendo {file}...")

        df = pd.read_csv(file_path, encoding="utf-8")

        print(f"✅ {name}: {df.shape[0]} linhas | {df.shape[1]} colunas")

        dataframes[name] = df

    return dataframes


if __name__ == "__main__":
    extract()
