from airflow import DAG
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
