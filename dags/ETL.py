from datetime import datetime
import os
import json
import pandas as pd 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow import DAG


def extract_and_transform_laod():
    # Spécifier le chemin vers le fichier CSV des urgences SOS médecins
    csv_file_path_sos = os.path.join(os.getenv("AIRFLOW_HOME"), "data", "donnees-urgences-SOS-medecins.csv")
    df_urgences = pd.read_csv(csv_file_path_sos, delimiter=';', dtype={'dep': str})

   
    csv_file_path = os.path.expandvars("${AIRFLOW_HOME}/data/code-tranches-dage-donnees-urgences.csv")
    df_tranches_dage = pd.read_csv(csv_file_path, delimiter=';')  
    print(df_tranches_dage)
    json_file_path_departements = os.path.expandvars("${AIRFLOW_HOME}/data/departements-region.json")
    with open(json_file_path_departements, 'r',encoding="UTF-8") as json_file:
        data = json.load(json_file)
        df_departements = pd.DataFrame(data)

    df_urgences = df_urgences.dropna(subset=['nbre_pass_tot'])

    # Convertir la colonne 'date_de_passage' en type datetime
    df_urgences['date_de_passage'] = pd.to_datetime(df_urgences['date_de_passage'], errors='coerce')

    # Supprimer les lignes avec des dates invalides
    df_urgences = df_urgences.dropna(subset=['date_de_passage'])

    #df_urgences["dep"] = pd.to_numeric(df_urgences["dep"], errors='coerce', downcast='string')
    df_urgences["dep"] = df_urgences["dep"].astype(str)
    #sup les colonne 
    colonnes_a_supprimer = ['nbre_acte_corona', 'nbre_acte_tot','nbre_acte_corona_h', 'nbre_acte_corona_f','nbre_acte_tot_h','nbre_acte_tot_f']
    df_urgences = df_urgences.drop(colonnes_a_supprimer, axis=1)
    print(df_urgences)
    # Remplacer 'A' par les valeurs numériques 1, 2, 3, 4, 5

    df_tranches_dage['Code tranches'] = df_tranches_dage['Code tranches'].replace({'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6})
    # Convertir la colonne en entiers
    df_tranches_dage['Code tranches'] = df_tranches_dage['Code tranches'].astype(int)
    df_tranches_dage.rename(columns={'Code tranches': 'code_tranches_dage'}, inplace=True)

    #df_departements["num_dep"] = pd.to_numeric(df_departements["num_dep"], errors='coerce', downcast='string')
    df_departements["num_dep"] = df_departements["num_dep"].astype(str)
    print(df_departements["num_dep"])

    postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connexion")
    df_urgences.to_sql('urgences', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000, index=False)
    df_tranches_dage.to_sql('tranches_dage', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='append', chunksize=1000,index=False)
    df_departements.to_sql('departements', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000,index=False)
    
    return 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(
    'Projet_IDD',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_connexion',
        sql='sql/create_table.sql'
    )
    # Utiliser un PythonOperator pour appeler la fonction extract_data
    extract_and_transform_task = PythonOperator(
        task_id='extract_and_transform_laod',
        python_callable=extract_and_transform_laod
    )

    # Utiliser un PythonOperator pour appeler la fonction transform_and_load
    create_key = PostgresOperator(
        task_id='create_key',
        postgres_conn_id='postgres_connexion',
        sql='sql/kly.sql'
    )

create_table>>extract_and_transform_task>>create_key
