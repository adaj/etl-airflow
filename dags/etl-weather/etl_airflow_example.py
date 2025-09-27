"""
ETL Airflow Example DAG - Daily Weather Report

This DAG demonstrates the same ETL process as etl_example.py but using Airflow features:
- Parameterized execution via UI
- Task dependencies and monitoring
- Error handling and retries
- Email notifications on failure/success
- Scheduling capabilities
- Real-time monitoring and logging

The DAG extracts weather data from OpenWeatherMap API, transforms it, and sends
a beautiful HTML email report with weather information.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from datetime import datetime, timedelta
import requests
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import ssl

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_weather_data(**context):
    """
    Extract current weather data from OpenWeatherMap API
    """
    params = context['params']
    city = params['city']
    api_key = params['api_key']
    
    print(f"🌤️  Extraindo dados do clima para {city}...")
    print(f"🔑 Usando API Key: {api_key[:8]}...")
    
    base_url = "http://api.openweathermap.org/data/2.5"
    current_url = f"{base_url}/weather"
    
    request_params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',
        'lang': 'pt_br'
    }
    
    try:
        response = requests.get(current_url, params=request_params, timeout=10)
        response.raise_for_status()
        weather_data = response.json()
        
        print(f"✅ Dados extraídos com sucesso!")
        print(f"📍 Cidade: {weather_data['name']}, {weather_data['sys']['country']}")
        print(f"🌡️  Temperatura: {weather_data['main']['temp']:.1f}°C")
        
        # Store data in XCom for next task
        return weather_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao extrair dados: {e}")
        raise Exception(f"Falha na extração de dados: {e}")

def transform_weather_data(**context):
    """
    Transform raw weather data into readable format
    """
    # Get data from previous task
    weather_data = context['task_instance'].xcom_pull(task_ids='extract_weather_data')
    params = context['params']
    
    print("🔄 Transformando dados...")
    
    try:
        # Extract relevant information
        transformed = {
            'city': weather_data['name'],
            'country': weather_data['sys']['country'],
            'temperature': weather_data['main']['temp'],
            'feels_like': weather_data['main']['feels_like'],
            'humidity': weather_data['main']['humidity'],
            'pressure': weather_data['main']['pressure'],
            'description': weather_data['weather'][0]['description'].title(),
            'wind_speed': weather_data['wind']['speed'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'execution_date': context['ds']
        }
        
        print("✅ Dados transformados com sucesso!")
        print(f"📊 Temperatura: {transformed['temperature']:.1f}°C")
        print(f"💧 Umidade: {transformed['humidity']}%")
        print(f"🌬️  Vento: {transformed['wind_speed']} m/s")
        
        return transformed
        
    except KeyError as e:
        print(f"❌ Erro ao transformar dados: Campo {e} não encontrado")
        raise Exception(f"Falha na transformação de dados: {e}")

def generate_html_report(**context):
    """
    Generate HTML report from transformed data
    """
    # Get data from previous task
    data = context['task_instance'].xcom_pull(task_ids='transform_weather_data')
    
    print("📝 Gerando relatório HTML...")
    
    # Weather emoji mapping
    weather_emojis = {
        'clear': '☀️',
        'clouds': '☁️',
        'rain': '🌧️',
        'snow': '❄️',
        'thunderstorm': '⛈️',
        'mist': '🌫️',
        'fog': '🌫️'
    }
    
    # Get appropriate emoji
    description_lower = data['description'].lower()
    emoji = '🌤️'  # default
    for key, value in weather_emojis.items():
        if key in description_lower:
            emoji = value
            break
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Relatório do Clima - {data['city']}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .container {{ max-width: 600px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            .header {{ text-align: center; color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
            .weather-info {{ margin: 20px 0; }}
            .metric {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #eee; }}
            .metric:last-child {{ border-bottom: none; }}
            .metric-label {{ font-weight: bold; color: #555; }}
            .metric-value {{ color: #333; }}
            .footer {{ text-align: center; margin-top: 20px; color: #666; font-size: 12px; }}
            .airflow-badge {{ background-color: #017CEE; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>{emoji} Relatório do Clima</h1>
                <h2>{data['city']}, {data['country']}</h2>
                <p>Atualizado em: {data['timestamp']}</p>
                <span class="airflow-badge">🚀 Powered by Apache Airflow</span>
            </div>
            
            <div class="weather-info">
                <div class="metric">
                    <span class="metric-label">🌡️ Temperatura:</span>
                    <span class="metric-value">{data['temperature']:.1f}°C</span>
                </div>
                <div class="metric">
                    <span class="metric-label">🤔 Sensação Térmica:</span>
                    <span class="metric-value">{data['feels_like']:.1f}°C</span>
                </div>
                <div class="metric">
                    <span class="metric-label">💧 Umidade:</span>
                    <span class="metric-value">{data['humidity']}%</span>
                </div>
                <div class="metric">
                    <span class="metric-label">📊 Pressão:</span>
                    <span class="metric-value">{data['pressure']} hPa</span>
                </div>
                <div class="metric">
                    <span class="metric-label">🌬️ Vento:</span>
                    <span class="metric-value">{data['wind_speed']} m/s</span>
                </div>
                <div class="metric">
                    <span class="metric-label">☁️ Condições:</span>
                    <span class="metric-value">{data['description']}</span>
                </div>
            </div>
            
            <div class="footer">
                <p>Relatório gerado automaticamente pelo ETL Weather System</p>
                <p>Dados fornecidos por OpenWeatherMap API</p>
                <p>Execução: {data['execution_date']}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    print("✅ Relatório HTML gerado!")
    return html_content

def send_weather_report(**context):
    """
    Send email with weather report using Airflow SMTP connection
    """
    params = context['params']
    html_content = context['task_instance'].xcom_pull(task_ids='generate_html_report')
    data = context['task_instance'].xcom_pull(task_ids='transform_weather_data')
    
    # Get SMTP connection from Airflow
    conn = BaseHook.get_connection('smtp_default')
    
    # Extract connection settings
    smtp_server = conn.host
    smtp_port = conn.port
    sender_email = conn.login
    sender_password = conn.password
    
    # Email settings
    recipient_email = params['recipient_email']
    city = params['city']
    
    subject = f"🌤️ Relatório do Clima - {city} - {context['ds']}"
    
    print(f"📧 Enviando email para {recipient_email}...")
    print(f"📎 Assunto: {subject}")
    
    # Create message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(html_content, 'html'))
    
    try:
        # Connect to SMTP server
        if smtp_port == 465:
            # Use SSL for port 465
            print("Usando SSL direto (porta 465)...")
            context_ssl = ssl.create_default_context()
            server = smtplib.SMTP_SSL(smtp_server, smtp_port, context=context_ssl)
        else:
            # Use STARTTLS for port 587
            print("Usando STARTTLS (porta 587)...")
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
        
        # Login and send
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        print("✅ Email enviado com sucesso!")
        return f"Relatório do clima enviado para {recipient_email}"
        
    except Exception as e:
        print(f"❌ Erro ao enviar email: {str(e)}")
        raise Exception(f"Falha ao enviar email: {str(e)}")

def etl_summary(**context):
    """
    Provide a summary of the ETL process
    """
    params = context['params']
    data = context['task_instance'].xcom_pull(task_ids='transform_weather_data')
    
    print("=" * 60)
    print("ETL WEATHER REPORT SUMMARY")
    print("=" * 60)
    print(f"🏙️  Cidade: {data['city']}, {data['country']}")
    print(f"🌡️  Temperatura: {data['temperature']:.1f}°C")
    print(f"💧 Umidade: {data['humidity']}%")
    print(f"🌬️  Vento: {data['wind_speed']} m/s")
    print(f"☁️  Condições: {data['description']}")
    print(f"📧 Email enviado para: {params['recipient_email']}")
    print(f"📅 Data de execução: {context['ds']}")
    print("=" * 60)
    print("🎉 Processo ETL concluído com sucesso!")

# DAG definition
with DAG(
    'etl_weather_report',
    default_args=default_args,
    description='ETL process to extract weather data and send daily reports',
    schedule='0 8 * * *',  # Run daily at 8 AM
    catchup=False,
    params={
        'city': Param(
            'São Paulo',
            type='string',
            description='City name for weather data (e.g., São Paulo, Rio de Janeiro)'
        ),
        'api_key': Param(
            'your_openweather_api_key',
            type='string',
            description='OpenWeatherMap API key (get free at openweathermap.org)'
        ),
        'recipient_email': Param(
            'recipient@example.com',
            type='string',
            description='Email address to receive the weather report'
        )
    },
    tags=['etl', 'weather', 'email', 'daily-report']
) as dag:

    # Task 1: Extract weather data
    extract_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data
    )

    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data
    )

    # Task 3: Generate HTML report
    generate_report_task = PythonOperator(
        task_id='generate_html_report',
        python_callable=generate_html_report
    )

    # Task 4: Send email report
    send_email_task = PythonOperator(
        task_id='send_weather_report',
        python_callable=send_weather_report
    )

    # Task 5: Summary
    summary_task = PythonOperator(
        task_id='etl_summary',
        python_callable=etl_summary
    )

    # Define task dependencies
    extract_task >> transform_task >> generate_report_task >> send_email_task >> summary_task
