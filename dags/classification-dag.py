from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix



# Реализуй DAG, который:

# Берет файл iris_scaled.csv и разделяет его на обучающую и валидационную части.
# Обучает и валидирует модель логистической регрессии.
# Сохраняет confusion_matrix в текстовый файл results.txt.

path = '../airflow/test_dir/iris_scaled.csv'
directory = '../airflow/test_dir'
target = 'target'

default_args = {
    "owner": 'Fyodor',
    'start_date': datetime(2025,10,14)
}

def df_train_test_split(**kwargs):
    df = pd.read_csv(path)
    features = [c for c in df.columns if c != target]
    X_train, X_test, y_train, y_test = train_test_split(
        df[features], df[target], test_size = 0.2, random_state=42, shuffle=True
    )

    return {
        'X_train' : X_train.values.tolist(),
        'X_test'  : X_test.values.tolist(),
        'y_train' : y_train.values.tolist(),
        'y_test'  : y_test.values.tolist(),
        'features' : features
     }

def train_and_test(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='train_test_split_dag')

    X_train = pd.DataFrame(data['X_train'], columns=data['features'])
    X_test = pd.DataFrame(data['X_test'], columns=data['features'])
    y_train = np.array(data['y_train'])
    y_test = np.array(data['y_test'])

    model = LogisticRegression(
        random_state=42
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    return {
        'y_test' : y_test.tolist(),
        'y_pred' : y_pred.tolist(),
        'classes': model.classes_.tolist()
    }

def calculate_matrix(**kwargs):
    ti = kwargs['ti']
    res = ti.xcom_pull(task_ids='train_and_test_dag')

    y_test = np.array(res['y_test'])
    y_pred = np.array(res['y_pred'])

    cm =  confusion_matrix(
        y_test, y_pred, labels= res['classes']
        )
    
    return {'cm': cm.tolist(), 'classes': res['classes']}


def create_txt(**kwargs):
    ti = kwargs['ti']
    out = ti.xcom_pull(task_ids='calculate_cm_dag')
    cm = out['cm']
    classes = out['classes']

    os.makedirs(directory, exist_ok=True)
    out_path = os.path.join(directory, 'confusion_matrix.txt')
    with open(out_path, 'a', encoding='utf-8') as f:
        f.write("Classes: " + ", ".join(map(str, classes)) + "\n")
        f.write("Confusion matrix:\n")
        for row in cm:
            f.write(" ".join(map(str, row)) + "\n")
        f.write("\n")


with DAG(
    dag_id = 'classification_dag',
    default_args = default_args,
    schedule=None
) as dag:

    split = PythonOperator(
        task_id = 'train_test_split_dag',
        python_callable = df_train_test_split
    )

    train_and_val = PythonOperator(
        task_id = 'train_and_test_dag',
        python_callable = train_and_test
    )

    calculate_cm = PythonOperator(
        task_id = 'calculate_cm_dag',
        python_callable = calculate_matrix
    )

    create_cm_txt = PythonOperator(
        task_id = 'create_cm_txt_dag',
        python_callable = create_txt
    )

split >> train_and_val >> calculate_cm >> create_cm_txt