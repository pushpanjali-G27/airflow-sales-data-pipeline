from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

INPUT_FILE = os.path.join(DATA_DIR, "airflow_sample_sales.csv")
STAGING_FILE = os.path.join(DATA_DIR, "staging_sales.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "processed_sales.csv")


def read_raw_data():
    df = pd.read_csv(INPUT_FILE)

    print(f"Raw records: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    df.to_csv(STAGING_FILE, index=False)


def clean_data():
    df = pd.read_csv(STAGING_FILE)

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Remove rows with missing important values
    df = df.dropna(
        subset=["order_id", "customer_name", "sales_amount"]
    )

    # Standardize order status
    df["order_status"] = df["order_status"].str.upper()

    # Remove invalid quantities
    df = df[df["quantity"] > 0]

    df.to_csv(STAGING_FILE, index=False)

    print(f"Records after cleaning: {len(df)}")


def validate_data():
    df = pd.read_csv(STAGING_FILE)

    if df["order_id"].isnull().any():
        raise ValueError(
            "Validation failed: order_id contains missing values"
        )

    if (df["quantity"] <= 0).any():
        raise ValueError(
            "Validation failed: invalid quantity found"
        )

    if (df["sales_amount"] < 0).any():
        raise ValueError(
            "Validation failed: negative sales amount found"
        )

    allowed_statuses = {
        "COMPLETED",
        "CANCELLED",
        "PENDING",
    }

    actual_statuses = set(
        df["order_status"].dropna().unique()
    )

    if not actual_statuses.issubset(allowed_statuses):
        raise ValueError(
            f"Validation failed: unexpected status found: {actual_statuses}"
        )

    print("Validation successful")
    print(f"Validated records: {len(df)}")


def save_data():
    df = pd.read_csv(STAGING_FILE)

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Processed file saved to: {OUTPUT_FILE}")
    print(f"Final records: {len(df)}")


with DAG(
    dag_id="sales_data_pipeline",
    start_date=datetime(2026, 9, 4),
    schedule=None,
    catchup=False,
) as dag:

    read_data = PythonOperator(
        task_id="read_raw_data",
        python_callable=read_raw_data,
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    save = PythonOperator(
        task_id="save_data",
        python_callable=save_data,
    )

    read_data >> clean >> validate >> save
