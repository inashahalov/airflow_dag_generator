from datetime import datetime

# Базовая конфигурация для генерируемых DAGs
DEFAULT_DAG_CONFIG = {
    'start_date': datetime.now(),
    'schedule_interval': '@daily',
    'catchup': False,
    'max_active_runs': 1,
    'tags': ['generated']
}