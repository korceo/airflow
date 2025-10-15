from __future__ import annotations
from airflow.decorators import dag, task
import os
from datetime import datetime

# Реализуй DAG с использованием двух операторов: PythonOperator и BashOperator:

# BashOperator создает папку в рабочей директории.
# PythonOperator создает txt-файл произвольного содержания.

# Перепиши вчерашние DAG-и в формате task-flow
# Имя py-файла: simple-dag-flow.py


BASE_DIR = '../airflow'
default_args = {
    'owner': 'Fyodor',
    'start_date': datetime(2025, 10, 15)
}

@task
def create_directory():
    directory = '<your_filepath>/test_dir'
    os.makedirs(directory, exist_ok=True)


@task
def create_file():
    directory = os.path.join(BASE_DIR, 'test_dir')
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, 'test.txt'), 'a') as f:
        f.write('Elbrus')


@dag(
    dag_id = 'simple_taskflow',
    default_args = default_args,
    schedule = None,
    catchup = False
)


def simple_taskflow():
    create_directory() >> create_file()

dag = simple_taskflow()