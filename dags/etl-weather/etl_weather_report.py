"""
ETL Airflow Example DAG - Daily Weather Report

This DAG demonstrates the same ETL process as etl_example.py but using Airflow features:
- Parameterized execution via UI
- Task dependencies and monitoring
- Error handling and retries
- Email notifications on failure/success
- Scheduling capabilities
- Real-time monitoring and logging

The DAG extracts weather data from OpenWeatherMap API, transforms it, and:
  1. Saves raw data to a CSV file (dags/etl-weather/weather_history.csv)
  2. Sends a beautiful HTML email report with weather information.

Configuration required in Airflow UI:
  - Variable  : owm_api_key  → your OpenWeatherMap API key
  - Connection: smtp_default → SMTP server settings for email delivery
    (see project docs / notify_user message for exact field mapping)
"""

import csv
import os
import smtplib
import ssl
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Default arguments for the DAG
# ---------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
}

# Path to the persistent CSV file inside the DAG folder
CSV_PATH = os.path.join(os.path.dirname(__file__), 'weather_history.csv')

# CSV column order
CSV_FIELDNAMES = [
    'timestamp',
    'city',
    'country',
    'temperature',
    'feels_like',
    'humidity',
    'pressure',
    'wind_speed',
    'description',
]


# ---------------------------------------------------------------------------
# Core ETL functions
# ---------------------------------------------------------------------------

def extract_weather_data(city: str, api_key: str) -> dict:
    """Extract current weather data from OpenWeatherMap API."""
    print(f"🌤️  Extraindo dados do clima para {city}...")

    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',
        'lang': 'pt_br',
    }

    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        weather_data = response.json()
        print("✅ Dados extraídos com sucesso!")
        return weather_data

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao extrair dados: {e}")
        raise


def transform_weather_data(weather_data: dict) -> dict:
    """Transform raw weather data into a readable, flat dict."""
    print("🔄 Transformando dados...")

    try:
        transformed = {
            'city': weather_data['name'],
            'country': weather_data['sys']['country'],
            'temperature': weather_data['main']['temp'],
            'feels_like': weather_data['main']['feels_like'],
            'humidity': weather_data['main']['humidity'],
            'pressure': weather_data['main']['pressure'],
            'description': weather_data['weather'][0]['description'].title(),
            'wind_speed': weather_data['wind']['speed'],
            # The current-weather endpoint returns a snapshot (not hourly),
            # so we record the full timestamp of when the data was captured.
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        print("✅ Dados transformados com sucesso!")
        return transformed

    except KeyError as e:
        print(f"❌ Erro ao transformar dados: Campo {e} não encontrado")
        raise


def save_to_csv(data: dict) -> str:
    """Append transformed weather data to the persistent CSV file.

    Returns the path of the CSV so it can be stored in XCom and referenced
    by downstream tasks or external tools.
    """
    print(f"💾 Salvando dados no CSV: {CSV_PATH}")

    file_exists = os.path.isfile(CSV_PATH)

    with open(CSV_PATH, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)

        # Write header only when creating the file for the first time
        if not file_exists:
            writer.writeheader()

        # Write only the columns defined in CSV_FIELDNAMES (drop any extras)
        row = {field: data.get(field, '') for field in CSV_FIELDNAMES}
        writer.writerow(row)

    print(f"✅ Dados salvos! ({CSV_PATH})")
    return CSV_PATH


def generate_html_report(data: dict) -> str:
    """Generate HTML report from transformed data."""
    print("📝 Gerando relatório HTML...")

    weather_emojis = {
        'clear': '☀️',
        'clouds': '☁️',
        'rain': '🌧️',
        'snow': '❄️',
        'thunderstorm': '⛈️',
        'mist': '🌫️',
        'fog': '🌫️',
    }

    description_lower = data['description'].lower()
    emoji = '🌤️'
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
            </div>
        </div>
    </body>
    </html>
    """

    print("✅ Relatório HTML gerado!")
    return html_content


def send_email(html_content: str, recipient_email: str, city: str, context: dict) -> None:
    """Send email with weather report using the Airflow 'smtp_default' connection.

    Connection setup (Admin → Connections → smtp_default):
      Connection Type : Email  (or Generic)
      Host            : smtp.gmail.com        (or your provider)
      Login           : your-sender@gmail.com
      Password        : your app-password
      Port            : 587  (STARTTLS) or 465 (SSL)
    """
    print(f"📧 Enviando email para {recipient_email}...")

    subject = f"🌤️ Relatório do Clima - {city} - {context['ds']}"

    conn = BaseHook.get_connection('smtp_default')
    smtp_server = conn.host
    smtp_port = int(conn.port)
    sender_email = conn.login
    sender_password = conn.password

    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(html_content, 'html'))

        if smtp_port == 465:
            print("Usando SSL direto (porta 465)...")
            context_ssl = ssl.create_default_context()
            server = smtplib.SMTP_SSL(smtp_server, smtp_port, context=context_ssl)
        else:
            print("Usando STARTTLS (porta 587)...")
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()

        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()

        print("✅ Email enviado com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao enviar email: {str(e)}")
        raise Exception(f"Falha ao enviar email: {str(e)}")


# ---------------------------------------------------------------------------
# Airflow Task wrappers
# ---------------------------------------------------------------------------

def extract_task(**context):
    """Airflow task wrapper for extract_weather_data.

    The API key is read from the Airflow Variable 'owm_api_key' so it never
    needs to appear in the DAG code or in the UI trigger form.
    """
    params = context['params']
    api_key = Variable.get('owm_api_key')
    weather_data = extract_weather_data(params['city'], api_key)
    return weather_data


def transform_task(**context):
    """Airflow task wrapper for transform_weather_data."""
    weather_data = context['task_instance'].xcom_pull(task_ids='extract_weather_data')
    transformed_data = transform_weather_data(weather_data)
    return transformed_data


def save_csv_task(**context):
    """Airflow task wrapper for save_to_csv."""
    data = context['task_instance'].xcom_pull(task_ids='transform_weather_data')
    csv_path = save_to_csv(data)
    return csv_path


def generate_report_task(**context):
    """Airflow task wrapper for generate_html_report."""
    data = context['task_instance'].xcom_pull(task_ids='transform_weather_data')
    html_content = generate_html_report(data)
    return html_content


def send_email_task(**context):
    """Airflow task wrapper for send_email."""
    params = context['params']
    html_content = context['task_instance'].xcom_pull(task_ids='generate_html_report')
    send_email(html_content, params['recipient_email'], params['city'], context)
    return f"Relatório do clima enviado para {params['recipient_email']}"


def summary_task(**context):
    """Provide a summary of the ETL process."""
    params = context['params']
    data = context['task_instance'].xcom_pull(task_ids='transform_weather_data')
    csv_path = context['task_instance'].xcom_pull(task_ids='save_weather_csv')

    print("=" * 60)
    print("ETL WEATHER REPORT SUMMARY")
    print("=" * 60)
    print(f"🏙️  Cidade: {data['city']}, {data['country']}")
    print(f"🌡️  Temperatura: {data['temperature']:.1f}°C")
    print(f"💧 Umidade: {data['humidity']}%")
    print(f"🌬️  Vento: {data['wind_speed']} m/s")
    print(f"☁️  Condições: {data['description']}")
    print(f"💾 CSV salvo em: {csv_path}")
    print(f"📧 Email enviado para: {params['recipient_email']}")
    print(f"📅 Data de execução: {context['ds']}")
    print("=" * 60)
    print("🎉 Processo ETL concluído com sucesso!")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    'etl_weather_report',
    default_args=default_args,
    description='ETL process to extract weather data, save to CSV, and send daily reports',
    schedule='0 8 * * *',  # Run daily at 8 AM
    catchup=False,
    params={
        'city': Param(
            'Natal',
            type='string',
            description='City name for weather data (e.g., São Paulo, Rio de Janeiro)',
        ),
        'recipient_email': Param(
            '[EMAIL_ADDRESS]',
            type='string',
            description='Email address to receive the weather report',
        ),
    },
    tags=['etl', 'weather', 'email', 'daily-report'],
) as dag:

    # Task 1: Extract weather data
    t_extract = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_task,
    )

    # Task 2: Transform data
    t_transform = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_task,
    )

    # Task 3: Save transformed data to CSV (Load step)
    t_save_csv = PythonOperator(
        task_id='save_weather_csv',
        python_callable=save_csv_task,
    )

    # Task 4: Generate HTML report
    t_generate_report = PythonOperator(
        task_id='generate_html_report',
        python_callable=generate_report_task,
    )

    # Task 5: Send email report
    t_send_email = PythonOperator(
        task_id='send_weather_report',
        python_callable=send_email_task,
    )

    # Task 6: Summary
    t_summary = PythonOperator(
        task_id='etl_summary',
        python_callable=summary_task,
    )

    # Task dependencies
    # After transform, CSV save and report generation can run in parallel
    t_extract >> t_transform >> [t_save_csv, t_generate_report]
    t_generate_report >> t_send_email
    [t_save_csv, t_send_email] >> t_summary