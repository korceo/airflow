from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
from datetime import datetime



# Реализуй DAG с использованием двух операторов: PythonOperator и BashOperator:

# BashOperator создает папку в рабочей директории.
# PythonOperator создает txt-файл произвольного содержания.
# Имя py-файла: simple-dag.py


BASE_DIR = '../airflow'

default_args = {
    'owner': 'Fyodor',
    'start_date': datetime(2025, 10, 14)
}

# def create_directory():
#     directory = '<your_filepath>/test_dir'
#     os.makedirs(directory, exist_ok=True)

def create_file():
    directory = os.path.join(BASE_DIR, 'test_dir')
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, 'test.txt'), 'a') as f:
        f.write('Elbrus')



with DAG(
    dag_id = 'simple_dag',
    default_args = default_args,
    schedule = None
) as dag:

    create_directory = BashOperator(
        task_id = 'create_dir',
        bash_command = 'set -euo pipefail; mkdir -p "{{ params.base_dir }}/test_dir"',
        params={'base_dir': BASE_DIR}
    )

    create_txt = PythonOperator(
        task_id = 'create_txt_file',
        python_callable = create_file
    )

create_directory >> create_txt