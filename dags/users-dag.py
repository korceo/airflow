from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, csv, requests

#  Реализуй DAG, который:

# Создаёт CSV-файл.
# Будет подключаться к randomuser.me/api раз в 10 минут2.
# Возвращенные json-данные будет записывать в созданный CSV-файл.
# Поля csv-файла:
# gender	first (name)	last (name)	city	country	latitude	longitude	username	age

path = '../airflow/test_dir/users.csv'
columns = ['gender', 'first (name)', 'last (name)', 'city',	'country',	'latitude',	'longitude','username', 'age']

default_args = {
    'owner' : 'Fyodor',
    'start_date': datetime(2025, 10, 14)
}

def fetch_and_append():
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_file = not os.path.exists(path)
    r = requests.get("https://randomuser.me/api/", timeout=10).json()["results"][0]
    row = {
        "gender": r["gender"],
        "first (name)": r["name"]["first"],
        "last (name)": r["name"]["last"],
        "city": r["location"]["city"],
        "country": r["location"]["country"],
        "latitude": r["location"]["coordinates"]["latitude"],
        "longitude": r["location"]["coordinates"]["longitude"],
        "username": r["login"]["username"],
        "age": r["dob"]["age"],
    }
    with open(PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        if new_file: w.writeheader()
        w.writerow(row)

with DAG(
    dag_id="users_dag",
    start_date=datetime(2025, 10, 14),
    default_args = default_args,
    schedule="*/10 * * * *",
    catchup=False,
) as dag:
    PythonOperator(task_id="fetch_and_append", python_callable=fetch_and_append)