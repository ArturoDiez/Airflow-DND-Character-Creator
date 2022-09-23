import random
from os.path import exists
import os
import requests
import airflow
import time
import datetime
import urllib.request as request
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': '@weekly',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

assignment_dag = DAG(
    dag_id='assignment_dag',
    default_args=default_args_dict,
    catchup=False,
)


def choose_class(url, output_folder, epoch):
    responseClase = requests.get(url=url)
    dataClase = responseClase.json()
    numberClase = random.randrange(0, int(dataClase['count']) - 1)
    clase = dataClase['results'][numberClase]['index']

    name = clase + epoch
    f = open(f'{output_folder}/name.txt', 'w')
    f.write(clase + '\n')
    f.write(name)
    f.close()


task_one = PythonOperator(
    task_id='choose_class',
    dag=assignment_dag,
    python_callable=choose_class,
    op_kwargs={
        'url': "https://www.dnd5eapi.co/api/classes",
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
        'urllang': "https://www.dnd5eapi.co/api/language",
    },
    trigger_rule='all_success',
)


def choose_race(url, output_folder):
    responseRace = requests.get(url=url)
    dataRace = responseRace.json()
    numberRace = random.randrange(0, int(dataRace['count']) - 1)
    race = dataRace['results'][numberRace]['index']

    f = open(f'{output_folder}/race.txt', 'w')
    f.write(race)
    f.close()


task_two = PythonOperator(
    task_id='choose_race',
    dag=assignment_dag,
    python_callable=choose_race,
    op_kwargs={
        'url': "https://www.dnd5eapi.co/api/races",
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
)


def choose_lang(url, output_folder):
    responseLang = requests.get(url=url)
    dataLang = responseLang.json()
    numberLang = random.randrange(0, int(dataLang['count']) - 1)
    lang = dataLang['results'][numberLang]['index']

    f = open(f'{output_folder}/lang.txt', 'w')
    f.write(lang)
    f.close()


task_three = PythonOperator(
    task_id='choose_lang',
    dag=assignment_dag,
    python_callable=choose_lang,
    op_kwargs={
        'url': "https://www.dnd5eapi.co/api/languages",
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
)


def levelAndAttributes(output_folder):
    level = random.randrange(1, 3)
    attributes = [0] * 6
    attributes[0] = random.randrange(6, 18)
    for i in range(1, 5):
        attributes[i] = random.randrange(2, 18)

    f = open(f'{output_folder}/levelandattr.txt', 'w')
    f.write(str(level) + '\n')
    f.write(str(attributes))
    f.close()


task_four = PythonOperator(
    task_id='levelAndAttributes',
    dag=assignment_dag,
    python_callable=levelAndAttributes,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
)


def proficiency_choice(output_folder):
    f = open(f'{output_folder}/name.txt', 'r')
    clase = f.readlines()[0].rstrip()

    responseProf = requests.get(url='https://www.dnd5eapi.co/api/classes/' + clase + '/proficiencies')
    dataProf = responseProf.json()
    numbersProf = random.sample(range(0, int(dataProf['count']) - 1), 2)
    prof = dataProf['results'][numbersProf[0]]['index']
    prof2 = dataProf['results'][numbersProf[1]]['index']

    f = open(f'{output_folder}/proficiencies.txt', 'w')
    f.write(prof + '\n')
    f.write(prof2)
    f.close()


task_five = PythonOperator(
    task_id='proficiency_choice',
    dag=assignment_dag,
    python_callable=proficiency_choice,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
)

join_tasks = DummyOperator(
    task_id='attributes_correct',
    dag=assignment_dag,
    trigger_rule='none_failed'
)


def spells_check(output_folder):
    f = open(f'{output_folder}/name.txt', 'r')
    clase = f.readlines()[0].rstrip()

    if clase == 'barbarian' or clase == 'fighter' or clase == 'monk' or clase == 'rogue':
        return 'put_on_database'
    else:
        return 'spells'


task_six = BranchPythonOperator(
    task_id='spells_check',
    dag=assignment_dag,
    python_callable=spells_check,
    op_kwargs={
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


def spells(output_folder):
    f = open(f'{output_folder}/name.txt', 'r')
    clase = f.readlines()[0].rstrip()

    f2 = open(f'{output_folder}/levelandattr.txt', 'r')
    level = int(f2.readlines()[0].rstrip()) + 3

    responseSpell = requests.get(url='https://www.dnd5eapi.co/api/classes/' + clase + '/levels/' + str(level) + '/spells')
    dataSpell = responseSpell.json()
    numbersSpell = random.sample(range(0, int(dataSpell['count']) - 1), 2)
    spell = dataSpell['results'][numbersSpell[0]]['index']
    spell2 = dataSpell['results'][numbersSpell[1]]['index']

    f = open(f'{output_folder}/spells.txt', 'w')
    f.write(spell + '\n')
    f.write(spell2)
    f.close()


task_seven = PythonOperator(
    task_id='spells',
    dag=assignment_dag,
    python_callable=spells,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
)


def create_query(output_folder):
    namef = open(f'{output_folder}/name.txt', 'r')
    racef = open(f'{output_folder}/race.txt', 'r')
    langf = open(f'{output_folder}/lang.txt', 'r')
    proff = open(f'{output_folder}/proficiencies.txt', 'r')
    levelf = open(f'{output_folder}/levelandattr.txt', 'r')
    spell = ''
    spell2 = ''

    if exists(f'{output_folder}/spells.txt'):
        spellsf = open(f'{output_folder}/spells.txt', 'r')
        spell = spellsf.readline()
        spell2 = spellsf.readline()

    with open('/opt/airflow/dags/pj_inserts.sql', "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS personajes (\n"
            "name VARCHAR(255),\n"
            "attributes VARCHAR(255),\n"
            "race VARCHAR(255),\n"
            "languages VARCHAR(255),\n"
            "class VARCHAR(255),\n"
            "proficiency VARCHAR(255)),\n"
            "level VARCHAR(255)),\n"
            "spells VARCHAR(255));\n"
        )

        race = racef.readline()
        languages = langf.readline()
        clase = namef.readline()
        name = namef.readline()
        prof = proff.readline()
        prof2 = proff.readline()
        level = levelf.readline()
        attributes = levelf.readline()

        f.write(
           "INSERT INTO personajes VALUES ("
           f"'{name}', '{attributes}', '{race}', '{languages}', '{clase}', '{prof + ' ' + prof2}, '{level}, '{spell + ' ' + spell2}'"
           ");\n"
        )

        f.close()
        os.remove(f'{output_folder}/spells.txt')


task_eight = PythonOperator(
    task_id='create_query',
    dag=assignment_dag,
    python_callable=create_query,
    op_kwargs={
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
)


task_nine = PostgresOperator(
    task_id='insert_query',
    dag=assignment_dag,
    postgres_conn_id='postgres_default',
    sql='pj_inserts.sql',
    trigger_rule='all_success',
    autocommit=True,
)

end = DummyOperator(
    task_id='end',
    dag=assignment_dag,
    trigger_rule='none_failed'
)

task_one >> task_five
[task_two, task_three, task_four, task_five] >> join_tasks
join_tasks >> task_six
task_six >> [task_seven, task_nine]
task_seven >> task_eight >> task_nine
task_nine >> end

