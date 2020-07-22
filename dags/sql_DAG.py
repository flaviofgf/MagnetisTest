from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.utils.dates import days_ago

args = {
    'owner':           'flaviofgf',
    'description':     'etl in sql',
    'start_date':      days_ago(1),
    'provide_context': True,
}

with DAG(
        dag_id='sql_DAG', default_args=args, schedule_interval='@daily', catchup=False, params={},
        template_searchpath=['/usr/local/airflow/sql/']
) as dag:
    init = BashOperator(
            task_id='init_sql_dag',
            bash_command='echo "Init SQL dag"',
    )
    
    create_table_raw_users = PostgresOperator(
            task_id='create_table_raw_users',
            sql='create_table_raw_users.sql'
    )
    
    create_table_users = PostgresOperator(
            task_id='create_table_users',
            sql='create_table_users.sql'
    )
    
    create_table_raw_funnel = PostgresOperator(
            task_id='create_table_raw_funnel',
            sql='create_table_raw_funnel.sql'
    )
    
    create_table_funnel = PostgresOperator(
            task_id='create_table_funnel',
            sql='create_table_funnel.sql'
    )
    
    extract_users_csv = PostgresOperator(
            task_id='extract_users_csv',
            sql='extract_users_csv.sql'
    )
    
    extract_funnel_csv = PostgresOperator(
            task_id='extract_funnel_csv',
            sql='extract_funnel_csv.sql'
    )
    
    refine_table_users = PostgresOperator(
            task_id='refine_table_users',
            sql='refine_table_users.sql'
    )
    
    refine_table_funnel = PostgresOperator(
            task_id='refine_table_funnel',
            sql='refine_table_funnel.sql'
    )
    
    get_result = PostgresOperator(
            task_id='get_result',
            sql='get_result.sql'
    )
    
    finish = BashOperator(
            task_id='finish_sql_dag',
            bash_command='echo "finish SQL dag"',
    )
    
    init >> create_table_raw_users >> extract_users_csv
    init >> create_table_raw_funnel >> extract_funnel_csv
    init >> create_table_users >> extract_users_csv
    init >> create_table_funnel >> extract_funnel_csv
    
    extract_users_csv >> refine_table_users >> get_result
    extract_funnel_csv >> refine_table_funnel >> get_result
    
    get_result >> finish
