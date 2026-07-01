from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

DATA_FILE = "/opt/airflow/data/customer.csv"
OUTPUT_DIR = "/opt/airflow/output"

def extract_customer(ti):

    df = pd.read_csv(DATA_FILE)

    ti.xcom_push(
        key="customers",
        value=df.to_dict("records")
    )

    print("Customer data extracted successfully")


def validate_customer(ti):

    customers = ti.xcom_pull(
        task_ids="extract_customers",
        key="customers"
    )

    if customers is None:
        raise ValueError(
            "No customer data found in XCom. Run extract_customers first."
        )

    valid_customers = []

    for customer in customers:
        if customer.get("name") and customer.get("email"):
            valid_customers.append(customer)

    print(f"Valid Customers: {len(valid_customers)}")

    ti.xcom_push(
        key="valid_customers",
        value=valid_customers
    )

def load_task(ti):

    valid_customers = ti.xcom_pull(
        task_ids="validate_customers",
        key="valid_customers"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(
        f"{OUTPUT_DIR}/valid_user.txt",
        "w"
    ) as f:

        for customer in valid_customers:
            f.write(
                f"Name: {customer['name']}, "
                f"Email: {customer['email']}\n"
            )

    print("Valid customer data written successfully")


def email_task(ti):

    valid_customers = ti.xcom_pull(
        task_ids="validate_customers",
        key="valid_customers"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(
        f"{OUTPUT_DIR}/email_sent.txt",
        "w"
    ) as f:

        for customer in valid_customers:
            f.write(
                f"Email sent to {customer['name']} "
                f"({customer['email']})\n"
            )

    print("Email notifications sent successfully")


with DAG(
    dag_id="customer_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customer
    )

    validate_customers = PythonOperator(
        task_id="validate_customers",
        python_callable=validate_customer
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_task
    )

    send_email = PythonOperator(
        task_id="send_email",
        python_callable=email_task
    )

    extract_customers >> validate_customers
    validate_customers >> load_data
    validate_customers >> send_email

