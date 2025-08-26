DAG_TEMPLATE = '''from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import logging

# Автоматически сгенерированный DAG из {source_file}

{function_imports}

{dag_definition}

{task_definitions}

# Определение зависимостей
{task_dependencies}
'''

PYTHON_FUNCTION_TEMPLATE = '''def {function_name}(**kwargs):
    """Автоматически сгенерированная функция из {source_file}"""
    try:
        logging.info("Начало выполнения функции {function_name}")
        {function_code}
        logging.info("Успешное завершение функции {function_name}")
    except Exception as e:
        logging.error(f"Ошибка в {function_name}: {{e}}")
        raise'''

SQL_QUERY_TEMPLATE = '''{query_name} = PostgresOperator(
    task_id='{task_id}',
    sql="""{sql_code}""",
    postgres_conn_id='postgres_default'
)'''