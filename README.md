# Airflow DAG Generator

## Описание проекта

Автоматический генератор DAG-файлов для Apache Airflow на основе Python и SQL скриптов.

# Генератор DAG'ов для Apache Airflow

## Описание проекта

Проект представляет собой локальное Python-приложение, предназначенное для автоматической генерации DAG'ов (Directed Acyclic Graphs) для Apache Airflow на основе файлов исходного кода. Инструмент анализирует Python и SQL скрипты в указанной директории и преобразует каждый файл в полнофункциональный DAG с оптимальной структурой тасок.

## Основные возможности

### Автоматическая генерация DAG'ов
- Каждый файл исходного кода превращается в отдельный DAG
- Поддержка Python (.py) и SQL (.sql) файлов
- Интеллектуальный парсинг исходных файлов для определения структуры

### Интеллектуальное разделение на таски
- **Для Python-скриптов**: каждая функция (`def`) становится отдельной таской
- **Для SQL-скриптов**: DDL-команды автоматически разделяются на отдельные таски
- Автоматическое определение зависимостей между тасками
- Оптимальное распределение нагрузки и последовательности выполнения

### Гибкое расписание
- Дата начала DAG устанавливается на день генерации
- Расписание задается непосредственно в исходном файле
- Поддержка cron-выражений и других форматов расписания Airflow

### Docker-поддержка
- Полностью готовая Docker-конфигурация для локального запуска
- Volume-монтирование для работы с внешними директориями
- Оптимизировано под существующую инфраструктуру

### Безопасность
- Параметры подключения не хранятся в сгенерированных DAG'ах
- Интеграция с механизмами безопасности Airflow (Connections, Variables)
- Соблюдение best practices по хранению конфиденциальных данных

## Преимущества

- **Экономия времени**: автоматизация рутинного процесса создания DAG'ов
- **Стандартизация**: единый подход к структуре DAG'ов в проекте
- **Гибкость**: расширяемая архитектура под различные сценарии использования
- **Надежность**: автоматическая обработка ошибок и логирование
- **Безопасность**: соответствие требованиям информационной безопасности

## Технические характеристики

- **Язык реализации**: Python 3.8+
- **Контейнеризация**: Docker и Docker Compose
- **Поддерживаемые форматы**: .py, .sql
- **Совместимость**: Apache Airflow 2.x
- **Лицензия**: Open Source

---

### Быстрый старт: Запуск проекта

1. **Установите Docker и Python 3.8+**  
   Убедитесь, что на вашем компьютере установлены:
   - [Docker](https://www.docker.com/)
   - Python 3.8 или выше

2. **Клонируйте репозиторий**
   ```bash
   git clone https://github.com/inashahalov/airflow_dag_generator.git
   cd airflow_dag_generator/airflow
   mkdir -p scripts/sql scripts/python data templates dags reports
   ```

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
6. 
   Перейдите в Admin → Connections
Нажмите кнопку "+" (Add record)
Заполните поля:
Connection Id: postgres_default
Connection Type: Postgres
Host: postgres (имя сервиса из docker-compose)
Schema: airflow (или оставьте пустым)
Login: airflow
Password: airflow
Port: 5432
Нажмите Save

    После настройки подключения вы можете проверить его в Airflow CLI:
```bash
    docker-compose exec airflow-worker airflow db check
```
## Примеры

### SQL DAG
Файл `sources/sql/ddl_example.sql`:
```sql
-- CONFIG: {"schedule_interval": "@daily", "tags": ["ddl", "database"]}

-- Создание таблицы пользователей
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы заказов
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индекса для ускорения поиска
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
```

Сгенерирует DAG с двумя последовательными задачами для выполнения DDL команд.
Файл `dags/ddl_generated.py`:
```py
with DAG(
    dag_id="ddl",
    start_date=datetime(2024, 1, 15),
    schedule="@daily",
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

```
## Требования

- Docker и Docker Compose
- Python 3.8+
- Apache Airflow 2.9.2

   
   python generate_dags.py
   ```

> После запуска скрипт `generate_dags.py` автоматически проанализирует файлы в папках `scripts/python` и `scripts/sql`, сгенерирует DAG'и и разместит их в директории `dags`.  
> Airflow будет автоматически загружать новые DAG'и при перезапуске.

---

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



## Скриншоты работы программы

### 1. Структура проекта
<img width="472" height="650" alt="image" src="https://github.com/user-attachments/assets/214e0fa8-2e99-494b-a70c-583290ef2874" />

### 2. Исходный Python файл
<img width="591" height="485" alt="image" src="https://github.com/user-attachments/assets/ff15f96c-f458-4f48-90aa-1d9a8176de73" />

### 3. Сгенерированный Python DAG
<img width="462" height="766" alt="image" src="https://github.com/user-attachments/assets/7c2710bc-d43d-4c48-86d0-39b037b2404b" />

### 4. Исходный SQL файл
<img width="609" height="509" alt="image" src="https://github.com/user-attachments/assets/0e51f91e-66a8-4b2f-90f7-2cfbce475a34" />

### 5. Сгенерированный SQL DAG
<img width="456" height="764" alt="image" src="https://github.com/user-attachments/assets/3d8f23a6-6f89-4166-8ad2-02a150caf4fe" />

### 6. DAG в Airflow UI
<img width="1831" height="360" alt="image" src="https://github.com/user-attachments/assets/a14f1d64-221f-4b8b-847f-666cd619af96" />


### 7. Граф представление DAG
<img width="1809" height="655" alt="image" src="https://github.com/user-attachments/assets/0461b719-290b-433f-8a13-e75af556431f" />


### 8. Логи выполнения задач
<img width="229" height="121" alt="image" src="https://github.com/user-attachments/assets/187ed165-b14c-48bf-954a-8799691c970f" />


### 9. Запуск генератора
<img width="1610" height="1035" alt="image" src="https://github.com/user-attachments/assets/3f768c63-cf3f-469c-82ab-89da3323501d" />

### 10. Docker Compose запуск
<img width="689" height="303" alt="image" src="https://github.com/user-attachments/assets/ceabd63f-fff8-49bd-8fde-3fcd4b095118" />


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
