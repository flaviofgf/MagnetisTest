from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator

from airflow.utils.dates import days_ago

args = {
    'owner':           'flaviofgf',
    'description':     'test',
    'start_date':      days_ago(1),
    'provide_context': True,
}

docker_operator_args = {
    'image':       'magnetis_test_spark:latest',
    'docker_url':  'host.docker.internal:2375',
    'volumes':     [
        'D:/Projects/magnetis_test/data:/home/jovyan/data',
        'D:/Projects/magnetis_test/work:/home/jovyan/work',
    ],
    'auto_remove': True,
    'tty':         True,
    'working_dir': '/home/jovyan/work',
}

with DAG(dag_id='etl_DAG', default_args=args, schedule_interval='@daily', catchup=False, params={}) as dag:
    init = BashOperator(
            task_id='init_etl_dag',
            bash_command='echo "Init ETL dag"',
    )
    
    refine_table_user = DockerOperator(
            task_id='refine_table_user',
            command='spark-submit refine_user.py',
            **docker_operator_args
    )
    
    refine_table_funnel = DockerOperator(
            task_id='refine_table_funnel',
            command='spark-submit refine_funnel.py',
            **docker_operator_args
    )
    
    get_result = DockerOperator(
            task_id='get_result',
            command='spark-submit get_result.py',
            **docker_operator_args
    )
    
    finish = BashOperator(
            task_id='finish_etl_dag',
            bash_command='echo "finish ETL dag"',
    )
    
    init >> refine_table_user
    init >> refine_table_funnel
    
    refine_table_user >> get_result
    refine_table_funnel >> get_result
    get_result >> finish
