import re
import json
from typing import List, Dict, Any
from datetime import datetime


class ScriptParser:
    @staticmethod
    def parse_python_script(file_path: str) -> Dict[str, Any]:
        """Парсит Python скрипт и извлекает функции и конфигурацию"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Извлечение конфигурации из комментариев
        config = ScriptParser._extract_config_from_comments(content)

        # Извлечение функций
        functions = ScriptParser._extract_functions(content)

        return {
            'functions': functions,
            'config': config,
            'type': 'python'
        }

    @staticmethod
    def parse_sql_script(file_path: str) -> Dict[str, Any]:
        """Парсит SQL скрипт и извлекает запросы"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Извлечение конфигурации из комментариев
        config = ScriptParser._extract_config_from_comments(content)

        # Разделение на DDL и DML команды
        queries = ScriptParser._extract_sql_queries(content)

        return {
            'queries': queries,
            'config': config,
            'type': 'sql'
        }

    @staticmethod
    def _extract_config_from_comments(content: str) -> Dict[str, Any]:
        """Извлекает конфигурацию из комментариев вида # CONFIG: {json}"""
        config_pattern = r'#\s*CONFIG:\s*(\{.*?\})'
        matches = re.findall(config_pattern, content, re.DOTALL)

        if matches:
            try:
                config = json.loads(matches[0])
                # Убедимся, что schedule_interval всегда строка
                if 'schedule_interval' in config:
                    config['schedule_interval'] = str(config['schedule_interval'])
                return config
            except json.JSONDecodeError:
                pass

        # Дефолтная конфигурация
        return {
            'schedule_interval': '@daily',
            'catchup': False
        }

    @staticmethod
    def _extract_functions(content: str) -> List[Dict[str, str]]:
        """Извлекает функции Python из кода"""
        functions = []

        # Находим все функции по паттерну def function_name(...
        function_pattern = r'def\s+(\w+)\s*\([^)]*\):(?:\s*""".*?"""|.*?)??\n(.*?)(?=\n(?:def\s+\w+|\Z))'
        matches = re.finditer(function_pattern, content, re.DOTALL)

        for match in matches:
            func_name = match.group(1)
            func_body = match.group(2)

            # Форматируем тело функции
            lines = func_body.strip().split('\n')
            formatted_lines = []
            for line in lines:
                if line.strip() and not line.strip().startswith('#'):
                    # Убираем лишние отступы и добавляем правильные
                    line_content = line.lstrip()
                    formatted_lines.append('    ' + line_content)

            if formatted_lines:
                function_code = '\n'.join(formatted_lines)
                functions.append({
                    'name': func_name,
                    'code': function_code
                })

        # Если не нашли функций по сложному паттерну, используем простой
        if not functions:
            simple_pattern = r'def\s+(\w+)\s*\([^)]*\):(.*?)(?=\n\w|\Z)'
            simple_matches = re.finditer(simple_pattern, content, re.DOTALL)

            for match in simple_matches:
                func_name = match.group(1)
                func_body = match.group(2)

                lines = func_body.strip().split('\n')
                formatted_lines = []
                for line in lines:
                    if line.strip() and not line.strip().startswith('#'):
                        formatted_lines.append('    ' + line.lstrip())

                if formatted_lines:
                    function_code = '\n'.join(formatted_lines)
                    functions.append({
                        'name': func_name,
                        'code': function_code
                    })

        return functions

    @staticmethod
    def _extract_sql_queries(content: str) -> List[Dict[str, str]]:
        """Разделяет SQL скрипт на отдельные запросы"""
        queries = []

        # Разделение по точке с запятой
        statements = re.split(r';\s*\n', content)

        for i, statement in enumerate(statements):
            statement = statement.strip()
            if statement and not statement.startswith('--') and statement != ';':
                # Убираем комментарии
                lines = statement.split('\n')
                clean_lines = [line for line in lines if not line.strip().startswith('--')]
                clean_statement = '\n'.join(clean_lines).strip()

                if clean_statement:
                    # Определение типа запроса
                    query_type = 'unknown'
                    statement_upper = clean_statement.upper().strip()

                    if any(ddl in statement_upper for ddl in ['CREATE', 'ALTER', 'DROP', 'TRUNCATE']):
                        query_type = 'ddl'
                    elif any(dml in statement_upper for dml in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
                        query_type = 'dml'

                    queries.append({
                        'name': f'statement_{i + 1}',
                        'code': clean_statement,
                        'type': query_type
                    })

        return queries