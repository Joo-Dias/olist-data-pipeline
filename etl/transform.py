import os
import pandas as pd
from etl.extract import extract

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

    # DIM CUSTOMER
    dim_customers = customers[[
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
    ]].drop_duplicates()

    # DIM PRODUCT
    dim_products = products[[
        "product_id",
        "product_category_name"
    ]].drop_duplicates()

    # DIM TIME
    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"]
    )

    dim_time = (
        orders[["order_purchase_timestamp"]]
        .drop_duplicates()
        .rename(columns={"order_purchase_timestamp": "date"})
    )

    dim_time["year"] = dim_time["date"].dt.year
    dim_time["month"] = dim_time["date"].dt.month
    dim_time["month_name"] = dim_time["date"].dt.month_name(locale="pt_BR")
    dim_time["day"] = dim_time["date"].dt.day

    dim_time["year_month"] = dim_time["date"].dt.strftime("%Y-%m")
    dim_time["quarter"] = dim_time["date"].dt.to_period("Q").astype(str)

    # Ordenação de datas
    dim_time = dim_time.sort_values("date")

    # FACT SALES
    fact_sales = (
        items.merge(orders, on="order_id", how="left").merge(payments, on="order_id", how="left")
    )

    fact_sales["total_value"] = (
        fact_sales["price"] + fact_sales["freight_value"]
    )

    # Salvar
    dim_customers.to_csv(f"{PROCESSED_PATH}/dim_customers.csv", index=False)
    dim_products.to_csv(f"{PROCESSED_PATH}/dim_products.csv", index=False)
    dim_time.to_csv(f"{PROCESSED_PATH}/dim_time.csv", index=False)
    fact_sales.to_csv(f"{PROCESSED_PATH}/fact_sales.csv", index=False)

    print("Modelagem dimensional concluída!")

if __name__ == "__main__":
    transform()
