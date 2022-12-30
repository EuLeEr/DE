from datetime import datetime
import psycopg2

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def hello():
    print("Airflow!")

def random():
    import random
    import os.path

    if os.path.exists("dags/random.txt"): 
        with open("dags/random.txt", "r") as f:
            lines = f.readlines()
        with open("dags/random.txt", "w") as f:
            for line in lines[:-1]:
                f.write(line)
        with open("dags/random.txt", "a") as f:
            f.write(str(random.randint(0, 100)))
            f.write(" ");
            f.write(str(random.randint(0, 100)))
            f.write("\n");

    else:
        with open("dags/random.txt", "w") as f:
            f.write(str(random.randint(0, 100)))
            f.write(" ");
            f.write(str(random.randint(0, 100)))
            f.write("\n");

def AllRandom():
    import subprocess
    import random


    sum1 = 0
    sum2 = 0
    with open("dags/random.txt", "r") as f:
        lines = f.readlines()   
    for line in lines:
        line = line.split(" ")
        sum1 = int(line[0]) + sum1
        sum2 = int(line[1]) + sum2
    with open("dags/random.txt", "a") as f:
        f.write(str(sum2-sum1))
        f.write("\n");

    
# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="Exersize_3_6_dag", start_date=datetime(2022, 1, 1), schedule="*/5 * * * *") as dag:
    


    # Tasks are represented as operators
    bash_task = BashOperator(task_id="hello", bash_command="pwd", do_xcom_push=False)
    python_task = PythonOperator(task_id="world", python_callable = hello, do_xcom_push=False)
    python_random = PythonOperator(task_id="random", python_callable = random, do_xcom_push=False)
    python_Summrandom = PythonOperator(task_id="SumRandom", python_callable = AllRandom, do_xcom_push=False)
    
    #set dependencies between tasks
    bash_task >> python_task >> python_random >> python_Summrandom

    Comment = '''conn_to_psql_tsk = PythonOperator(task_id="conn_to_psql", python_callable = connect_to_psql)
    read_from_psql_tsk = PythonOperator(task_id="read_from_psql", python_callable = read_from_psql)

    sql_sensor = SqlSensor(
            task_id='sql_sensor_check',
            poke_interval=60,
            timeout=180,
            soft_fail=False,
            retries=2,
            sql="select count(*) from test_table",
            conn_id=Variable.get("conn_id"),
        dag=dag)

    bash_task2 = BashOperator(task_id="bye", bash_command="echo bye, baby, bye", do_xcom_push=False)
    
    choose_best_model = BranchPythonOperator(
        task_id = 'branch_oper',
        python_callable = python_branch,
        do_xcom_push = False
    )

    # Set dependencies between tasks
    bash_task >> python_task >> conn_to_psql_tsk >> read_from_psql_tsk >> sql_sensor \
         >> bash_task2 >> choose_best_model >> [accurate, inaccurate]    '''


