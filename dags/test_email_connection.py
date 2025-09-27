"""
DAG de teste para verificar configuração de email usando conexões do Airflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import ssl

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 0,
}

def send_email_with_connection(**context):
    """
    Envia email usando conexão do Airflow (sem hardcode de credenciais)
    """
    # Obter conexão do Airflow
    conn = BaseHook.get_connection('smtp_default')
    
    # Extrair configurações da conexão
    smtp_server = conn.host
    smtp_port = conn.port
    sender_email = conn.login
    sender_password = conn.password
    
    # Configurações do email
    recipient_email = "adelson.dias@gmail.com"
    
    print(f"Usando servidor: {smtp_server}:{smtp_port}")
    print(f"Email remetente: {sender_email}")
    
    # Criar mensagem
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "Teste de Email - Airflow (Conexão)"
    
    # Corpo do email
    body = """
    <h3>✅ Email funcionando com conexão do Airflow!</h3>
    <p>Se você recebeu este email, a configuração está correta.</p>
    <p>Este email foi enviado usando as credenciais da conexão smtp_default.</p>
    """
    
    msg.attach(MIMEText(body, 'html'))
    
    try:
        if smtp_port == 465:
            # Usar SSL direto para porta 465
            print("Usando SSL direto (porta 465)...")
            context_ssl = ssl.create_default_context()
            server = smtplib.SMTP_SSL(smtp_server, smtp_port, context=context_ssl)
        else:
            # Usar STARTTLS para porta 587
            print("Usando STARTTLS (porta 587)...")
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
        
        # Habilitar modo debug
        server.set_debuglevel(1)
        
        # Fazer login
        print("Fazendo login...")
        server.login(sender_email, sender_password)
        
        # Enviar email
        print("Enviando email...")
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        
        # Fechar conexão
        server.quit()
        
        print("✅ Email enviado com sucesso usando conexão do Airflow!")
        return "Email enviado com sucesso usando conexão do Airflow!"
        
    except Exception as e:
        print(f"❌ Erro ao enviar email: {str(e)}")
        raise Exception(f"Falha ao enviar email: {str(e)}")

with DAG(
    'test_email_connection',
    default_args=default_args,
    description='Teste de envio de email usando conexão do Airflow',
    schedule=None,
    catchup=False,
) as dag:

    send_test_email = PythonOperator(
        task_id='send_test_email_connection',
        python_callable=send_email_with_connection
    )
