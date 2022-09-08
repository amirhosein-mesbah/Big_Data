
from datetime import datetime, timedelta
from textwrap import dedent
from crawl_news import news_crawler

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Operators; we need this to operate!

with DAG(
    'news_crawl_dag',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2022, 6, 24),
    catchup=False,
    tags=['example'],
) as dag:

    create_news_df = PythonOperator(task_id='create_news_df',
                        python_callable=news_crawler,
                        dag=dag)


    create_news_df 
