import os
import pandas as pd
from extract import extract

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed")

os.makedirs(PROCESSED_PATH, exist_ok=True)

def transform():
    dfs = extract()

    orders = dfs["orders"]
    items = dfs["items"]
    payments = dfs["payments"]
    customers = dfs["customers"]
    products = dfs["products"]

    # 🕒 Tratamento de datas
    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"]
    )

    # 💰 Fato de vendas (base)
    fact_sales = (
        items
        .merge(orders, on="order_id", how="left")
        .merge(payments, on="order_id", how="left")
    )

    fact_sales["total_value"] = (
        fact_sales["price"] + fact_sales["freight_value"]
    )

    # 💾 Salvar base fato
    fact_sales.to_csv(
        os.path.join(PROCESSED_PATH, "fact_sales.csv"),
        index=False
    )

    print("✅ Transformação concluída com sucesso!")

if __name__ == "__main__":
    transform()
