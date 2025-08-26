import os
import sys
from pathlib import Path
from datetime import datetime
import importlib.util

# Добавляем пути к модулям
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(project_dir, 'utils')
config_dir = os.path.join(project_dir, 'config')
templates_dir = os.path.join(project_dir, 'templates')

# Загружаем модули динамически
parser_spec = importlib.util.spec_from_file_location("parser", os.path.join(utils_dir, "parser.py"))
parser_module = importlib.util.module_from_spec(parser_spec)
parser_spec.loader.exec_module(parser_module)
ScriptParser = parser_module.ScriptParser

config_spec = importlib.util.spec_from_file_location("dag_config", os.path.join(config_dir, "dag_config.py"))
config_module = importlib.util.module_from_spec(config_spec)
config_spec.loader.exec_module(config_module)
DEFAULT_DAG_CONFIG = config_module.DEFAULT_DAG_CONFIG

templates_spec = importlib.util.spec_from_file_location("dag_template", os.path.join(templates_dir, "dag_template.py"))
templates_module = importlib.util.module_from_spec(templates_spec)
templates_spec.loader.exec_module(templates_module)
DAG_TEMPLATE = templates_module.DAG_TEMPLATE
PYTHON_FUNCTION_TEMPLATE = templates_module.PYTHON_FUNCTION_TEMPLATE
SQL_QUERY_TEMPLATE = templates_module.SQL_QUERY_TEMPLATE


class DAGGenerator:
    def __init__(self, sources_dir='sources', dags_dir='dags'):
        self.sources_dir = Path(sources_dir)
        self.dags_dir = Path(dags_dir)
        self.parser = ScriptParser()

        # Создаем директории если их нет
        self.sources_dir.mkdir(exist_ok=True)
        self.dags_dir.mkdir(exist_ok=True)

        # Создаем поддиректории для источников
        (self.sources_dir / 'python').mkdir(exist_ok=True)
        (self.sources_dir / 'sql').mkdir(exist_ok=True)

    def scan_sources(self):
        """Сканирует директории с исходниками и возвращает список файлов"""
        source_files = []

        # Сканируем Python файлы
        python_dir = self.sources_dir / 'python'
        if python_dir.exists():
            for file_path in python_dir.glob('*.py'):
                source_files.append({
                    'path': file_path,
                    'type': 'python'
                })

        # Сканируем SQL файлы
        sql_dir = self.sources_dir / 'sql'
        if sql_dir.exists():
            for file_path in sql_dir.glob('*.sql'):
                source_files.append({
                    'path': file_path,
                    'type': 'sql'
                })

        return source_files

    def generate_dag_from_python(self, file_info):
        """Генерирует DAG из Python скрипта"""
        parsed_data = self.parser.parse_python_script(file_info['path'])

        # Извлечение имени файла для DAG ID
        dag_id = file_info['path'].stem

        # Конфигурация DAG
        dag_config = dict(DEFAULT_DAG_CONFIG)
        dag_config.update(parsed_data.get('config', {}))
        dag_config['start_date'] = datetime.now()  # Всегда текущая дата

        # Генерация функций
        function_imports = []
        task_definitions = []
        task_names = []

        for func in parsed_data['functions']:
            # Создаем функцию с правильными отступами
            function_lines = []
            function_lines.append('def {func_name}(**kwargs):'.format(func_name=func['name']))
            function_lines.append('    """Автоматически сгенерированная функция из {source_file}"""'.format(
                source_file=file_info['path'].name))
            function_lines.append('    try:')
            function_lines.append(
                '        logging.info("Начало выполнения функции {func_name}")'.format(func_name=func['name']))

            # Добавляем код функции с правильными отступами
            func_code_lines = func['code'].split('\n')
            for line in func_code_lines:
                if line.strip() and not line.strip().startswith('#'):
                    # Убираем существующие отступы и добавляем правильные
                    clean_line = line.lstrip()
                    if clean_line:
                        function_lines.append('        ' + clean_line)
                elif line.strip().startswith('#'):
                    # Комментарии тоже добавляем с правильными отступами
                    clean_line = line.lstrip()
                    function_lines.append('        ' + clean_line)

            function_lines.append(
                '        logging.info("Успешное завершение функции {func_name}")'.format(func_name=func['name']))
            function_lines.append('    except Exception as e:')
            function_lines.append(
                '        logging.error(f"Ошибка в {func_name}: {{e}}")'.format(func_name=func['name']))
            function_lines.append('        raise')

            function_code = '\n'.join(function_lines)
            function_imports.append(function_code)

            # Создаем задачу
            task_def = ('{func_name}_task = PythonOperator(\n'
                        '    task_id=\'{func_name}\',\n'
                        '    python_callable={func_name},\n'
                        ')').format(func_name=func['name'])
            task_definitions.append(task_def)
            task_names.append("{func_name}_task".format(func_name=func['name']))

        # Формирование зависимостей (последовательное выполнение)
        task_dependencies = ''
        if len(task_names) > 1:
            dependencies = []
            for i in range(len(task_names) - 1):
                dependencies.append("{prev_task} >> {next_task}".format(
                    prev_task=task_names[i],
                    next_task=task_names[i + 1]
                ))
            task_dependencies = '\n'.join(dependencies)

        # Определение DAG - исправляем булевые значения
        dag_definition = ('with DAG(\n'
                          '    dag_id="{dag_id}",\n'
                          '    start_date=datetime({year}, {month}, {day}),\n'
                          '    schedule_interval="{schedule_interval}",\n'
                          '    catchup={catchup},\n'
                          '    max_active_runs={max_active_runs},\n'
                          '    tags={tags}\n'
                          ') as dag:\n'
                          '    pass').format(
            dag_id=dag_id,
            year=dag_config['start_date'].year,
            month=dag_config['start_date'].month,
            day=dag_config['start_date'].day,
            schedule_interval=dag_config['schedule_interval'],
            catchup=str(dag_config['catchup']).capitalize(),  # Исправляем на True/False
            max_active_runs=dag_config['max_active_runs'],
            tags=dag_config.get('tags', ['generated'])
        )

        # Сборка финального DAG
        dag_content = DAG_TEMPLATE.format(
            source_file=file_info['path'].name,
            function_imports='\n\n'.join(function_imports).strip(),
            dag_definition=dag_definition.strip(),
            task_definitions='\n\n'.join(task_definitions).strip(),
            task_dependencies=task_dependencies.strip()
        )

        return dag_content

    def generate_dag_from_sql(self, file_info):
        """Генерирует DAG из SQL скрипта"""
        parsed_data = self.parser.parse_sql_script(file_info['path'])

        # Извлечение имени файла для DAG ID
        dag_id = file_info['path'].stem

        # Конфигурация DAG
        dag_config = dict(DEFAULT_DAG_CONFIG)
        dag_config.update(parsed_data.get('config', {}))
        dag_config['start_date'] = datetime.now()  # Всегда текущая дата

        # Генерация задач для SQL запросов
        task_definitions = []
        task_names = []

        for i, query in enumerate(parsed_data['queries']):
            task_id = "{name}_{type}".format(name=query['name'], type=query['type'])
            # Экранирование фигурных скобок для Jinja
            escaped_sql = query['code'].replace('{', '{{').replace('}', '}}')

            task_def = SQL_QUERY_TEMPLATE.format(
                query_name=query['name'],
                task_id=task_id,
                sql_code=escaped_sql
            )
            task_definitions.append(task_def)
            task_names.append(query['name'])

        # Формирование зависимостей (последовательное выполнение)
        task_dependencies = ''
        if len(task_names) > 1:
            dependencies = []
            for i in range(len(task_names) - 1):
                dependencies.append("{prev_task} >> {next_task}".format(
                    prev_task=task_names[i],
                    next_task=task_names[i + 1]
                ))
            task_dependencies = '\n'.join(dependencies)

        # Определение DAG - исправляем булевые значения
        dag_definition = ('with DAG(\n'
                          '    dag_id="{dag_id}",\n'
                          '    start_date=datetime({year}, {month}, {day}),\n'
                          '    schedule_interval="{schedule_interval}",\n'
                          '    catchup={catchup},\n'
                          '    max_active_runs={max_active_runs},\n'
                          '    tags={tags}\n'
                          ') as dag:\n'
                          '    pass').format(
            dag_id=dag_id,
            year=dag_config['start_date'].year,
            month=dag_config['start_date'].month,
            day=dag_config['start_date'].day,
            schedule_interval=dag_config['schedule_interval'],
            catchup=str(dag_config['catchup']).capitalize(),  # Исправляем на True/False
            max_active_runs=dag_config['max_active_runs'],
            tags=dag_config.get('tags', ['generated'])
        )

        # Сборка финального DAG
        dag_content = DAG_TEMPLATE.format(
            source_file=file_info['path'].name,
            function_imports='# SQL задачи определены ниже',
            dag_definition=dag_definition.strip(),
            task_definitions='\n\n'.join(task_definitions).strip(),
            task_dependencies=task_dependencies.strip()
        )

        return dag_content

    def generate_all_dags(self):
        """Генерирует все DAGs из исходных файлов"""
        source_files = self.scan_sources()

        if not source_files:
            print("Нет исходных файлов для генерации DAGs")
            return

        generated_count = 0

        for file_info in source_files:
            try:
                if file_info['type'] == 'python':
                    dag_content = self.generate_dag_from_python(file_info)
                elif file_info['type'] == 'sql':
                    dag_content = self.generate_dag_from_sql(file_info)
                else:
                    continue

                # Сохранение DAG файла
                dag_filename = "{stem}_generated.py".format(stem=file_info['path'].stem)
                dag_filepath = self.dags_dir / dag_filename

                with open(dag_filepath, 'w', encoding='utf-8') as f:
                    f.write(dag_content)

                print("Сгенерирован DAG: {filename}".format(filename=dag_filename))
                generated_count += 1

            except Exception as e:
                print("Ошибка при генерации DAG из {path}: {error}".format(
                    path=file_info['path'], error=str(e)))
                import traceback
                traceback.print_exc()

        print("Всего сгенерировано DAGs: {count}".format(count=generated_count))


def main():
    """Основная функция для запуска генератора"""
    generator = DAGGenerator()
    generator.generate_all_dags()


if __name__ == "__main__":
    main()