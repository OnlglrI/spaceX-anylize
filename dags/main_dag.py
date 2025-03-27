from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from sqlalchemy.orm import Session
import value_in_database as v
import logging
import create_database as c
import json

class K:
    HOST = "https://api.spacexdata.com/v4"


def load_data(func,url,ps_conn):
    hook = PostgresHook(postgres_conn_id = ps_conn)
    engine = hook.get_sqlalchemy_engine()
    json_list = json.loads(v.get_data(url))
    c.Base.metadata.create_all(engine)
    with Session(engine) as session:
        for js in json_list:
            try:
                session.add(func(js))
                session.commit()
            except Exception as e:
                if "already exists" in str(e):
                    logger.info(">>>>>>>> уже в  таблице")
                else:
                    raise Exception(f"Ошикбка {e}")
                    

logger = logging.getLogger(__name__)


dag = DAG(
        dag_id = "my_dag",
        start_date = days_ago(5),
        schedule_interval = None,
        catchup = False)


add_capsules_to_table = PythonOperator(
	task_id = "add_capsules_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_capsules,
		"url": f"{K.HOST}/capsules",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_cores_to_table = PythonOperator(
	task_id = "add_cores_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_cores,
		"url": f"{K.HOST}/cores",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_crew_to_table = PythonOperator(
	task_id = "add_crew_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_crew,
		"url": f"{K.HOST}/crew",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_landpads_to_table = PythonOperator(
	task_id = "add_landpads_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_landpads,
		"url": f"{K.HOST}/landpads",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_launches_to_table = PythonOperator(
	task_id = "add_launches_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_launches,
		"url": f"{K.HOST}/launches",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_launchpads_to_table = PythonOperator(
	task_id = "add_launchpads_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_launchpads,
		"url": f"{K.HOST}/launchpads",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_payloads_to_table = PythonOperator(
	task_id = "add_payloads_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_payloads,
		"url": f"{K.HOST}/payloads",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_rockets_to_table = PythonOperator(
	task_id = "add_rockets_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_rockets,
		"url": f"{K.HOST}/rockets",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_ships_to_table = PythonOperator(
	task_id = "add_ships_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_ships,
		"url": f"{K.HOST}/ships",
		"ps_conn": "postgres_db"
	},
	dag=dag
)

add_starlink_to_table = PythonOperator(
	task_id = "add_starlink_to_table",
	python_callable = load_data,
	op_kwargs = {
		"func": v.get_starlink,
		"url": f"{K.HOST}/starlink",
		"ps_conn": "postgres_db"
	},
	dag=dag
)
(
add_capsules_to_table    
>> add_cores_to_table
>> add_crew_to_table
>> add_landpads_to_table
>> add_launches_to_table
>> add_launchpads_to_table
>> add_payloads_to_table
>> add_rockets_to_table
>> add_ships_to_table
>> add_starlink_to_table
)
