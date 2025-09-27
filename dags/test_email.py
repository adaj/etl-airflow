"""
DAG de teste para verificar configuração de email
"""

from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 0,
    # 'retry_delay': timedelta(seconds=10)
}

with DAG(
    'test_email',
    default_args=default_args,
    description='Teste simples de envio de email',
    schedule=None,
    catchup=False,
) as dag:

    send_test_email = EmailOperator(
        task_id='send_test_email',
        conn_id='smtp_default',
        to='adelson.dias@gmail.com',  # Seu email
        subject='Teste de Email - Airflow',
        html_content='<h3>✅ Email funcionando!</h3><p>Se você recebeu este email, a configuração está correta.</p>',
    )
