from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

def task1_start():
    print(f"Transaction pipeline has started at {datetime.now()}")

def task2_process_transactions():
    transactions = [120, 300, 450, 200]
    count = len(transactions)
    total = sum(transactions)

    with open("/data/transactions_report.txt", "w") as f:
        f.write("Daily Transaction Report\n")
        f.write(f"Number of transactions: {count}\n")
        f.write(f"Total amount: {total}\n")

def task4_read_report():
    with open("/data/transactions_report.txt", "r") as f:
        return f.read()

with DAG(
    dag_id="transaction_pipeline",
    start_date=datetime(2026,4,4),
    schedule_interval=timedelta(minutes=2),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="task1_start",
        python_callable=task1_start
    )

    task2 = PythonOperator(
        task_id="task2_process_transactions",
        python_callable=task2_process_transactions
    )

    task3 = BashOperator(
        task_id="task3_run_script",
        bash_command="python /data/process_transactions.py"
    )

    task4 = PythonOperator(
        task_id="task4_read_report",
        python_callable=task4_read_report
    )

    task5 = EmailOperator(
        task_id="task5_send_email",
        to="hayarisha28@gmail.com",
        subject="Transaction Report",
        html_content="{{ ti.xcom_pull(task_ids='task4_read_report') }}"
    )

    task1 >> task2 >> task3 >> task4 >> task5