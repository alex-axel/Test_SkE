import os
import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

# переменная, задающая путь к sql-файлам
SQL_LOCATION = os.path.join("/usr/local/airflow/dags/sql")

# аргументы дага: начало выполнения, количество попыток перезапуска, 
# интервал между перезапусками, зависимость от пердыдущих запусков
args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 4, 18, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

# при инициализации дага указываем имя, путь к sql-файлам
# и периодичность запуска: даг будет запускаться каждую 42-ю минуту часа
dag = DAG(
    'skyeng',
    default_args=args,
    template_searchpath=SQL_LOCATION,
    schedule_interval='42 * * * *'
)

def copy_pg_to_csv(sql, file_name):
    '''
        Функция экспорта результатов sql-зароса в CSV-файл,
        разделенный запятыми. Файл сохраняется локально.
        Алгоритм работы:
        получаем соединение с postgres с помощью хука,
        вызываем метод хука copy_export, который позволяет 
        экспортировать данные в файл.
    '''
    filename = os.path.join("/home/administrator/filestorage/", file_name)
    pg_hook = PostgresHook.get_hook("postgres_mock")
    logging.info(f"Exporting query to file {filename}")
    pg_hook.copy_expert(f"copy ({sql}) to stdout delimiter ',' csv header", filename=filename)

# оператор, выполняющий функцию экспорта из postgres в csv
# в op_kwargs заданы аргументы, передаваемые в функцию copy_pg_to_csv
# в sql можно дописать условие where, чтобы не выгружать каждый раз всю таблицу orders
# например, select * from orders where created_at::date = current_date or updated_at::date = current_date

copy_orders_from_pg_to_csv = PythonOperator(
    task_id="copy_pg_to_csv",
    python_callable=copy_pg_to_csv,
    dag=dag,
    op_kwargs={
        "sql": "select * from orders", 
        "file_name": "orders.csv",
    }
)

# оператор, выполняющий sql-код из файла merge_orders.sql
# в Greenplum для БД dwh

merge_orders_in_gpdb = PostgresOperator(
    task_id='merge_orders_in_gpdb',
    postgres_conn_id='greenplum',
    sql='merge_orders.sql',
    database='dwh',
    dag=dag
)

# задается зависимость задач друг от друга
# если задача экспорта данных завершается ошибкой, 
# то задача обновленя заказов в dwh также не будет выполнена
copy_orders_from_pg_to_csv >> merge_orders_in_gpdb