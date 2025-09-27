"""
DAG de teste para verificar configuração de email usando PythonOperator
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 0,
}

def send_email_directly(**context):
    """
    Envia email diretamente usando smtplib para contornar problemas do EmailOperator
    """
    # Configurações SMTP
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "adelson.dias@gmail.com"
    sender_password = "..."  # Substitua pela sua senha de app
    recipient_email = "adelson.dias@gmail.com"
    
    # Criar mensagem
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "Teste de Email - Airflow (PythonOperator)"
    
    # Corpo do email
    body = """
    <h3>✅ Email funcionando!</h3>
    <p>Se você recebeu este email, a configuração está correta.</p>
    <p>Este email foi enviado usando PythonOperator diretamente.</p>
    """
    
    msg.attach(MIMEText(body, 'html'))
    
    try:
        # Conectar ao servidor SMTP
        print(f"Conectando ao servidor SMTP: {smtp_server}:{smtp_port}")
        server = smtplib.SMTP(smtp_server, smtp_port)
        
        # Habilitar modo debug para ver o que está acontecendo
        server.set_debuglevel(1)
        
        # Iniciar TLS
        print("Iniciando TLS...")
        server.starttls()
        
        # Fazer login
        print("Fazendo login...")
        server.login(sender_email, sender_password)
        
        # Enviar email
        print("Enviando email...")
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        
        # Fechar conexão
        server.quit()
        
        print("✅ Email enviado com sucesso!")
        return "Email enviado com sucesso!"
        
    except Exception as e:
        print(f"❌ Erro ao enviar email: {str(e)}")
        raise Exception(f"Falha ao enviar email: {str(e)}")

with DAG(
    'test_email_python',
    default_args=default_args,
    description='Teste de envio de email usando PythonOperator',
    schedule=None,
    catchup=False,
) as dag:

    send_test_email = PythonOperator(
        task_id='send_test_email_python',
        python_callable=send_email_directly
    )
