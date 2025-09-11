#!/usr/bin/env python3
"""
Простой скрипт для создания тестовых DAG файлов
"""


def create_etl_dag():
    """Создает тестовый ETL DAG"""
    dag_content = '''from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Автоматически сгенерированный DAG из etl.py

def extract_data(**kwargs):
    """Автоматически сгенерированная функция из etl.py"""
    try:
        logging.info("Начало выполнения функции extract_data")
        logging.info("Извлечение данных из источника")
        # Ваш код извлечения данных
        data = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        logging.info(f"Извлечено {{len(data)}} записей")
        return data
    except Exception as e:
        logging.error(f"Ошибка в extract_data: {{e}}")
        raise

def transform_data(**kwargs):
    """Автоматически сгенерированная функция из etl.py"""
    try:
        logging.info("Начало выполнения функции transform_data")
        logging.info("Трансформация данных")
        # Ваш код трансформации
        logging.info("Данные трансформированы")
    except Exception as e:
        logging.error(f"Ошибка в transform_data: {{e}}")
        raise

def load_data(**kwargs):
    """Автоматически сгенерированная функция из etl.py"""
    try:
        logging.info("Начало выполнения функции load_data")
        logging.info("Загрузка данных в хранилище")
        # Ваш код загрузки
        logging.info("Данные успешно загружены")
    except Exception as e:
        logging.error(f"Ошибка в load_data: {{e}}")
        raise

with DAG(
    dag_id="etl",
    start_date=datetime(2024, 1, 15),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=['generated', 'etl', 'example']
) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Определение зависимостей
    extract_data_task >> transform_data_task >> load_data_task
'''

    with open('dags/etl.py', 'w', encoding='utf-8') as f:
        f.write(dag_content)

    print("Создан DAG: etl.py")


def create_ddl_dag():
    """Создает тестовый DDL DAG"""
    dag_content = '''from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import logging

# Автоматически сгенерированный DAG из ddl.sql

with DAG(
    dag_id="ddl",
    start_date=datetime(2024, 1, 15),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['generated', 'ddl', 'database']
) as dag:

    statement_1_ddl = PostgresOperator(
        task_id='statement_1_ddl',
        sql="""CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        postgres_conn_id='postgres_default'
    )

    statement_2_ddl = PostgresOperator(
        task_id='statement_2_ddl',
        sql="""CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        amount DECIMAL(10,2) NOT NULL,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        postgres_conn_id='postgres_default'
    )

    statement_3_ddl = PostgresOperator(
        task_id='statement_3_ddl',
        sql="""CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);""",
        postgres_conn_id='postgres_default'
    )

    # Определение зависимостей
    statement_1_ddl >> statement_2_ddl >> statement_3_ddl
'''

    with open('dags/ddl.py', 'w', encoding='utf-8') as f:
        f.write(dag_content)

    print("Создан DAG: ddl.py")


def main():
    """Основная функция"""
    print("Создание тестовых DAG файлов...")

    # Создаем директорию dags если её нет
    import os
    os.makedirs('dags', exist_ok=True)

    create_etl_dag()
    create_ddl_dag()

    print("Все DAG файлы успешно созданы!")


if __name__ == "__main__":
    main()