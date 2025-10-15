from __future__ import annotations
from airflow.decorators import task, dag
from datetime import datetime
from sklearn.datasets import load_iris
import pandas as pd
from sklearn.preprocessing import StandardScaler
import os


# Реализуй DAG, который:

# Скачивает датасет iris по ссылке (взять можно тут).
# Применяет к признакам StandartScaler из sklearn.
# Сохраняет файл под названием iris_scaled.csv.
# Имя py-файла: scaling-dag-flow.py


path = '../airflow/test_dir'
default_args = {
    'owner': 'Fyodor',
    'start_date': datetime(2025, 10, 15)
}

@task
def download_iris():
    os.makedirs(path, exist_ok=True)
    df = load_iris(as_frame=True).frame 
    df.to_csv(os.path.join(path, 'iris.csv'), index=False)

@task
def scale_iris():
    iris = os.path.join(path, 'iris.csv')
    scaled = os.path.join(path, 'iris_scaled.csv')
    df = pd.read_csv(iris)
    features = [c for c in df.columns if c != 'target']
    scaler = StandardScaler()
    df[features] = scaler.fit_transform(df[features])
    df.to_csv(scaled, index=False)
    

@dag(
    dag_id = 'scaling-taskflow',
    default_args = default_args,
    schedule = None,
    catchup = False,
)

def scaling_taskflow():
    download_iris() >> scale_iris()

dag = scaling_taskflow()