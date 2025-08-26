# Генератор DAG для Apache Airflow

## Описание проекта

Проект представляет собой автоматический генератор DAG (Directed Acyclic Graph) для Apache Airflow, который создает рабочие графы на основе исходных Python и SQL скриптов. 

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

### 1. Клонирование репозитория
```bash
git clone <repository-url>
cd airflow_dag_generator
```

### 2. Сборка Docker образа
```bash
docker build -t airflow-with-java .
```

### 3. Создание исходных файлов

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

### 4. Генерация DAG
```bash
python generate_dags.py
```

### 5. Запуск Airflow
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



## Автор

Илья Нашахалов
