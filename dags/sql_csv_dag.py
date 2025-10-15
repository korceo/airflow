# total_revenue.csv - таблица общей выручки с полетов по дням
# daily_departed.csv - таблица количества вылетевших рейсов по дням
# daily_arrived.csv - таблица прилетевших рейсов по дням


from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from datetime import datetime
import csv
import logging
from pathlib import Path


CONN_ID = 'postgres18'
total_revenue_csv_path = '../airflow/test_dir/total_revenue.csv'
daily_departed_csv_path = '../airflow/test_dir/daily_departed.csv'
daily_arrived_csv_path = '../airflow/test_dir/daily_arrived.csv'

total_revenue_sql = """
select sum(tf.amount) as revenue, f.scheduled_departure::date as date
from ticket_flights tf 
join flights f on f.flight_id  = tf.flight_id 
group by date
order by date asc
"""

daily_departed_sql = """
select count(f.flight_id) as cnt, f.actual_departure::date as date
from flights f 
group by date
order by date asc
"""

daily_arrived_sql = """
select count(f.flight_id) as cnt, f.actual_arrival::date as date
from flights f
group by date
order by date asc
"""

default_args = {
    'owner': 'Fyodor',
    'start_date': datetime(2025, 10, 15)
}

def sql_to_csv(sql, csv_path):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            headers = [d[0] for d in cur.description]
    
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    
    logging.info("Saved %d rows to %s", len(rows), str(path))
    

@dag(
    dag_id = 'export_csvs_dag',
    default_args=default_args,
    schedule=None,
    catchup= False
)

def export_csvs_dag():
    @task(task_id='export_total_revenue_csv')
    def export_total_revenue_csv():
        sql_to_csv(total_revenue_sql, total_revenue_csv_path)

    @task(task_id ='export_daily_departed_csv')
    def export_daily_departed_csv():
        sql_to_csv(daily_departed_sql, daily_departed_csv_path)

    @task(task_id='export_daily_arrived_csv')
    def export_daily_arrived_csv():
        sql_to_csv(daily_arrived_sql, daily_arrived_csv_path)
    
    export_total_revenue_csv() >> export_daily_departed_csv() >> export_daily_arrived_csv()

dag = export_csvs_dag()