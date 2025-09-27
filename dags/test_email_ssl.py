"""
DAG de teste para verificar configuração de email usando porta 465 (SSL)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
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

def send_email_ssl(**context):
    """
    Envia email usando porta 465 com SSL
    """
    # Configurações SMTP
    smtp_server = "smtp.gmail.com"
    smtp_port = 465  # Porta SSL
    sender_email = "adelson.dias@gmail.com"
    sender_password = "..."  # Substitua pela sua senha de app
    recipient_email = "adelson.dias@gmail.com"
    
    # Criar mensagem
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "Teste de Email - Airflow (SSL 465)"
    
    # Corpo do email
    body = """
    <h3>✅ Email funcionando com SSL!</h3>
    <p>Se você recebeu este email, a configuração SSL está correta.</p>
    <p>Este email foi enviado usando porta 465 com SSL.</p>
    """
    
    msg.attach(MIMEText(body, 'html'))
    
    try:
        # Criar contexto SSL
        context_ssl = ssl.create_default_context()
        
        # Conectar ao servidor SMTP com SSL
        print(f"Conectando ao servidor SMTP com SSL: {smtp_server}:{smtp_port}")
        server = smtplib.SMTP_SSL(smtp_server, smtp_port, context=context_ssl)
        
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
        
        print("✅ Email enviado com sucesso usando SSL!")
        return "Email enviado com sucesso usando SSL!"
        
    except Exception as e:
        print(f"❌ Erro ao enviar email: {str(e)}")
        raise Exception(f"Falha ao enviar email: {str(e)}")

with DAG(
    'test_email_ssl',
    default_args=default_args,
    description='Teste de envio de email usando SSL (porta 465)',
    schedule=None,
    catchup=False,
) as dag:

    send_test_email = PythonOperator(
        task_id='send_test_email_ssl',
        python_callable=send_email_ssl
    )
