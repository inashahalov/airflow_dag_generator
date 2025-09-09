# Airflow DAG Generator

## Описание проекта

Автоматический генератор DAG-файлов для Apache Airflow на основе Python и SQL скриптов. Решает проблему рутинного создания DAG-файлов для аналитических скриптов.

### Особенности:
- **Поддержка Python и SQL**: Генерирует DAG из обоих типов скриптов
- **Автоматическое разделение на таски**: Python функции и SQL команды автоматически разделяются на отдельные задачи
- **Конфигурация через исходники**: Время и частота запуска задаются в самих исходных файлах
- **Безопасность**: Параметры подключения не хранятся в DAG
- **Docker-совместимость**: Полностью настроен под конфигурацию Docker

## Структура проекта

```
airflow_dag_generator/
├── dags/                    # Сгенерированные DAG файлы
├── sources/                 # Исходные файлы
│   ├── python/              # Python скрипты
│   └── sql/                 # SQL скрипты
├── generator/               # Генератор DAG
│   └── dag_generator.py     # Основной генератор
├── utils/                   # Вспомогательные утилиты
│   └── parser.py           # Парсер исходных файлов
├── config/                  # Конфигурации
│   └── dag_config.py       # Базовые настройки
├── templates/              # Шаблоны
│   └── dag_template.py     # Шаблоны DAG
├── Dockerfile              # Docker конфигурация
├── docker-compose.yml      # Docker Compose конфигурация
├── requirements.txt        # Зависимости
└── generate_dags.py        # Скрипт генерации
```

## Установка и запуск

### 1. Создание исходных файлов

#### Python скрипт (sources/python/example.py):
```python
# CONFIG: {"schedule_interval": "@hourly", "tags": ["etl", "example"]}

def extract_data():
    """Извлечение данных из источника"""
    print("Извлечение данных...")
    # Ваш код извлечения

def transform_data():
    """Трансформация данных"""
    print("Трансформация данных...")
    # Ваш код трансформации

def load_data():
    """Загрузка данных"""
    print("Загрузка данных...")
    # Ваш код загрузки
```

#### SQL скрипт (sources/sql/example.sql):
```sql
-- CONFIG: {"schedule_interval": "@daily", "tags": ["ddl", "database"]}

-- Создание таблицы пользователей
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL
);

-- Создание таблицы заказов
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id)
);
```

### 2. Генерация DAG
```bash
python generate_dags.py
```

### 3. Запуск Airflow
```bash
docker-compose up --build
```

## Конфигурация

### Параметры в исходных файлах
Конфигурация DAG задается в комментариях исходных файлов:

```python
# CONFIG: {"schedule_interval": "@hourly", "tags": ["my", "tags"]}
```

Доступные параметры:
- `schedule_interval`: Частота запуска (`@daily`, `@hourly`, `*/30 * * * *` и т.д.)
- `tags`: Теги DAG
- `catchup`: Флаг догоняющего выполнения (по умолчанию `False`)

### Подключение к базам данных
Параметры подключения задаются через Airflow Connections и не хранятся в DAG файлах.

## Использование

1. **Добавьте свои скрипты** в папки `sources/python/` или `sources/sql/`
2. **Запустите генератор**:
   ```bash
   python generate_dags.py
   ```
3. **Запустите Airflow**:
   ```bash
   docker-compose up
   ```
4. **Откройте интерфейс** по адресу `http://localhost:8080`
5. **Войдите** с логином и паролем `airflow`

## Примеры

### Python DAG
Файл `sources/python/etl_example.py`:
```python
# CONFIG: {"schedule_interval": "@hourly", "tags": ["etl"]}

def extract():
    """Извлечение данных"""
    return [{"id": 1, "name": "John"}]

def transform():
    """Трансформация данных"""
    pass

def load():
    """Загрузка данных"""
    pass
```

Сгенерирует DAG с тремя последовательными задачами: extract → transform → load

### SQL DAG
Файл `sources/sql/ddl_example.sql`:
```sql
-- CONFIG: {"schedule_interval": "@daily", "tags": ["ddl"]}

CREATE TABLE users (id SERIAL PRIMARY KEY);
CREATE INDEX idx_users ON users(id);
```

Сгенерирует DAG с двумя последовательными задачами для выполнения DDL команд.

## Требования

- Docker и Docker Compose
- Python 3.8+
- Apache Airflow 2.9.2

## Скриншоты работы программы

### 1. Структура проекта
<img width="472" height="650" alt="image" src="https://github.com/user-attachments/assets/214e0fa8-2e99-494b-a70c-583290ef2874" />

### 2. Исходный Python файл
<img width="591" height="485" alt="image" src="https://github.com/user-attachments/assets/ff15f96c-f458-4f48-90aa-1d9a8176de73" />

### 3. Сгенерированный Python DAG
ddl_generated.py

<img width="610" height="687" alt="image" src="https://github.com/user-attachments/assets/6e642a3a-e963-4e10-bda7-78f78458bb99" />

etl_generated.py

<img width="579" height="682" alt="image" src="https://github.com/user-attachments/assets/7183b8ed-902a-45d1-86d6-c020392afdeb" />
<img width="522" height="482" alt="image" src="https://github.com/user-attachments/assets/f02c1a33-a304-4790-9f00-f3fc20b5ad8b" />



### 4. Исходный SQL файл
![SQL скрипт](docs/screenshots/sql_source.png)

### 5. Сгенерированный SQL DAG
![Сгенерированный SQL DAG](docs/screenshots/generated_sql_dag.png)

### 6. DAG в Airflow UI
![DAG в интерфейсе](docs/screenshots/airflow_ui_dag.png)

### 7. Граф представление DAG
![Граф DAG](docs/screenshots/dag_graph.png)

### 8. Логи выполнения задач
![Логи задач](docs/screenshots/task_logs.png)

### 9. Запуск генератора
![Запуск генератора](docs/screenshots/generator_run.png)

### 10. Docker Compose запуск
![Docker Compose](docs/screenshots/docker_compose.png)

## Архитектура решения

### Общая схема:
```
Исходники → Парсер → Шаблонизатор → DAG-файлы
```

### Компоненты:
1. **Парсер исходных файлов**: Анализирует Python и SQL скрипты
2. **Генератор DAG**: Создает DAG-файлы на основе шаблонов
3. **Шаблонизатор**: Использует Jinja2 для генерации кода
4. **Конфигуратор**: Извлекает параметры из исходных файлов



## Автор

Илья Нашахалов
