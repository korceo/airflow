from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sklearn.datasets import load_iris
import pandas as pd
from sklearn.preprocessing import StandardScaler
import os


# Реализуй DAG, который:

# Скачивает датасет iris по ссылке (взять можно тут).
# Применяет к признакам StandartScaler из sklearn.
# Сохраняет файл под названием iris_scaled.csv.
# Имя py-файла: scaling-dag.py


path = '../airflow/test_dir'
default_args = {
    'owner': 'Fyodor',
    'start_date': datetime(2025, 10, 14)
}


def download_iris():
    os.makedirs(path, exist_ok=True)
    df = load_iris(as_frame=True).frame 
    df.to_csv(os.path.join(path, 'iris.csv'), index=False)


def scale_iris():
    iris = os.path.join(path, 'iris.csv')
    scaled = os.path.join(path, 'iris_scaled.csv')
    df = pd.read_csv(iris)
    features = [c for c in df.columns if c != 'target']
    scaler = StandardScaler()
    df[features] = scaler.fit_transform(df[features])
    df.to_csv(scaled, index=False)
    

with DAG(
    dag_id = 'download_and_scale_iris',
    default_args = default_args,
    schedule = None
) as dag:

    download_iris = PythonOperator(
        task_id = 'download_iris',
        python_callable = download_iris
    )

    scale_iris = PythonOperator(
        task_id = 'scale_iris',
        python_callable = scale_iris
    )


download_iris >> scale_iris