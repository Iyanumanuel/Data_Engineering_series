from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models import Variable
from datetime import datetime, timedelta
#from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
#from airflow.operators.email_operator import EmailOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
#from airflow.utils.email import send_email

airbyte_connection_id = Variable.get("AIRBYTE_CRIME_CONNECTION_ID")

default_args = {
        'owner': 'airflow',
        'email': 'e.iyanuloluwa@gmail.com',
        'email_on_failure':True
}
with DAG(dag_id='airbyte_crime_data_etl',
         default_args=default_args,
         schedule_interval='@hourly',
         start_date=days_ago(0),
         dagrun_timeout=300
         ) as dag:

    example_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_crime_data_etl',
        airbyte_conn_id='AIRBYTE_CRIME_CONNECTION_ID',
        connection_id=airbyte_connection_id,
        asynchronous=False,
        timeout=60,
        wait_seconds=3
     )