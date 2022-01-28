# Importando as bibliotecas que vamos usar nesse exemplo
import os
import psycopg2
import pandas as pd
import zipfile
import pymongo as pym
import json
from sqlalchemy import create_engine
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

# Cria as funções para executar na dag
def CreateCanadaCovid():

   conn_string = 'postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db'
   conn = create_engine(conn_string).connect()

   file_path = '/tmp/kaggle/Canada_Hosp_COVID19_Inpatient_DatasetDefinitions/'

   df = pd.read_excel(file_path + '/Canada_Hosp1_COVID_InpatientData.xlsx')
   df = df.filter(['id','reason_for_admission','age','sex','medications'])

   df.to_sql(
      'canada_covid',
      con = conn,
      if_exists='replace',
      index=False
   )

def CreateCanadaCovidMongo():

   file_path = '/home/hadoop/Datasets/kaggle/Canada_Hosp_COVID19_Inpatient_DatasetDefinitions/'
   df_kaggle = pd.read_excel(file_path + '/Canada_Hosp1_COVID_InpatientData.xlsx')
   df_kaggle = df_kaggle.filter(['id','age','sex','medications'])

   df_kaggle['medications'] = df_kaggle['medications'].apply(lambda x : x[1:-1])
   df_kaggle['medications'] = df_kaggle['medications'].apply(lambda x : x.replace('\\',''))

   df_kaggle.dropna(inplace = True)

   df_kaggle_dict = df_kaggle.to_dict("records")

   # Making a Connection to MongoClient
   client = pym.MongoClient('mongodb://localhost:27017/')

   # CREATING A DATABASE
   db = client["kaggle"]

   # CREATING A COLLECTION
   collection = db["canada_covid"]

   collection.drop()

   data_json = json.loads(df_kaggle.to_json(orient='records'))
   db.canada_covid.insert_many(data_json)

   client.close()


def UnzipFile(path):

   files=os.listdir(path)
   for file in files:
      if file.endswith('.zip'):
         filePath=path+'/'+file
         zip_file = zipfile.ZipFile(filePath)
         for names in zip_file.namelist():
            zip_file.extract(names,path)
         zip_file.close() 


# Definindo alguns argumentos básicos
defaultArgs = {
   'owner': 'airflow',
   'depends_on_past': False,
   'start_date': datetime(2022, 1, 25),
   'retries': 3,
   'retry_delay': timedelta(minutes=1)
   }


# Criando a DAG e definindo quando ela vai ser executado

dag = DAG(
   'kaggle_etl', 
   schedule_interval='@daily', 
   default_args=defaultArgs, 
   catchup=False
)


# Criando as tasks

create_kaggle_dir = BashOperator(
    task_id='create_kaggle_dir',
    bash_command='mkdir -p /tmp/kaggle',
    dag=dag
)

download_kaggle_file = BashOperator(
   task_id='download_kaggle_file',
   bash_command='kaggle datasets download -d roche-data-science-coalition/uncover -p /tmp/kaggle',
   dag=dag
)

unzip_file = PythonOperator(
   task_id='unzip_file',
   python_callable=UnzipFile,
   op_kwargs={'path': '/tmp/kaggle'},
   dag=dag
)

create_table_postgre = PythonOperator(
   task_id="create_table_postgre",
   python_callable=CreateCanadaCovid,
   dag=dag
)

dummy = DummyOperator(
   task_id="dummy",
   dag=dag
)

create_table_mongo = PythonOperator(
   task_id="create_table_mongo",
   python_callable=CreateCanadaCovidMongo,
   dag=dag
)

drop_kaggle_dir = BashOperator(
    task_id='drop_kaggle_dir',
    bash_command='rm -rf /tmp/kaggle',
    dag=dag
)

qtd_by_reason = BashOperator(
   task_id='qtd_by_reason',
   bash_command='cd /opt/airflow/dags/api && spark-submit app_qtdByReason.py',
   dag=dag
)

qtd_by_age = BashOperator(
   task_id='qtd_by_age',
   bash_command='cd /opt/airflow/dags/api && spark-submit app_percentByAge.py',
   dag=dag
)

qtd_by_agesex = BashOperator(
   task_id='qtd_by_agesex',
   bash_command='cd /opt/airflow/dags/api && spark-submit app_percentByAgeSex.py',
   dag=dag
)

# Definindo o padrão de execução

create_kaggle_dir >> download_kaggle_file >> unzip_file >> [create_table_postgre, create_table_mongo] >> dummy >> [qtd_by_reason, qtd_by_age, qtd_by_agesex] >> drop_kaggle_dir