from datetime import datetime
import sys
import csv
import json
import os
import pandas as pd 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator

def data_transform_and_load():
    #spécifier le chemin vers le fichier CSV
    csv_file_path_sos = os.path.expandvars("${AIRFLOW_HOME}/data/donnees-urgences-SOS-medecins.csv")
    df_urgences = pd.read_csv(csv_file_path_sos, delimiter=';')
   
    csv_file_path = os.path.expandvars("${AIRFLOW_HOME}/data/code-tranches-dage-donnees-urgences.csv")
    df_tranches_dage = pd.read_csv(csv_file_path, delimiter=',')  
    
    json_file_path_departements = os.path.expandvars("${AIRFLOW_HOME}/data/departements-region.json")
    with open(json_file_path_departements, 'r',encoding="UTF-8") as json_file:
        data = json.load(json_file)
        df_departements = pd.DataFrame(data)
        
    #nettoyage 
    df_tranches_dage ['Code_tranches_dage'] = df_tranches_dage['Code_tranches_dage'].astype(int) 
    df_urgences['date_de_passage'] = pd.to_datetime(df_urgences['date_de_passage'])
    print('---------kabyle---------------------')

# Suppose df_departements est votre DataFrame
    df_departements["num_dep"] = pd.to_numeric(df_departements["num_dep"], errors='coerce', downcast='integer')
    df_departements["num_dep"] = df_departements["num_dep"].astype('Int64')  # Utilisation de 'Int64' pour permettre les valeurs nulles (NaN)

    print(df_departements['num_dep'].dtypes)
    print('---------kabyle---------------------')
    
   #print(df_urgences.dtypes)
   #print(df_departements.dtypes) 
    


   
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(
    'Projet',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

# Utiliser un PythonOperator pour appeler la fonction extract_data
    transform_and_load = PythonOperator(
        task_id='transform_and_load',
        python_callable= data_transform_and_load
    )
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_connexion',
        sql='sql/create_table.sql'
    )
   
create_table >> transform_and_load 
   
   
   
