"""
ETL Airflow DAG - Weather Forecast Collector

Roda a cada 3 horas e coleta:
  1. Clima atual via GET /data/2.5/weather
  2. Todos os slots de previsão (~40 entradas, 3h cada) via GET /data/2.5/forecast

Cada slot de forecast é salvo como uma linha em forecast_data.csv, contendo:
  - captured_at  : timestamp da execução (clima atual)
  - current_*    : dados do clima no momento da coleta (repetido por slot)
  - forecast_for : timestamp do slot previsto
  - forecast_*   : dados previstos para aquele slot

Isso permite avaliar a acurácia do forecast: comparar o forecast_* de uma run
anterior com o current_* da run cujo captured_at coincide com aquele forecast_for.

Configuração necessária no Airflow UI:
  - Variable  : owm_api_key      → chave da API OpenWeatherMap
  - Variable  : owm_target_city  → cidade-alvo (ex: "Natal", "São Paulo")
                                   Pode ser alterada a qualquer momento no UI;
                                   a próxima execução agendada usará o novo valor.
"""

import csv
import os
from datetime import datetime, timezone

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

CSV_PATH = os.path.join(os.path.dirname(__file__), 'forecast_data.csv')

CSV_FIELDNAMES = [
    'captured_at',
    'city',
    'country',
    # --- clima atual no momento da coleta ---
    'current_temp',
    'current_feels_like',
    'current_humidity',
    'current_pressure',
    'current_wind_speed',
    'current_description',
    # --- previsão para o slot correspondente ---
    'forecast_for',
    'forecast_temp',
    'forecast_feels_like',
    'forecast_humidity',
    'forecast_pressure',
    'forecast_wind_speed',
    'forecast_description',
]

OWM_BASE = 'https://api.openweathermap.org/data/2.5'


# ---------------------------------------------------------------------------
# Funções de extração
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    return Variable.get('owm_api_key')


def _get_city() -> str:
    return Variable.get('owm_target_city')


def extract_current(**context):
    """Chama /weather e retorna o snapshot do clima atual."""
    city = _get_city()
    api_key = _get_api_key()
    print(f"🌤️  Extraindo clima atual para '{city}'...")

    response = requests.get(
        f'{OWM_BASE}/weather',
        params={'q': city, 'appid': api_key, 'units': 'metric', 'lang': 'pt_br'},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    print(f"✅ Clima atual extraído: {data['main']['temp']:.1f}°C, {data['weather'][0]['description']}")
    return data


def extract_forecast(**context):
    """Chama /forecast e retorna todos os slots de previsão (~40 entradas)."""
    city = _get_city()
    api_key = _get_api_key()
    print(f"🔭 Extraindo forecast para '{city}'...")

    response = requests.get(
        f'{OWM_BASE}/forecast',
        params={'q': city, 'appid': api_key, 'units': 'metric', 'lang': 'pt_br'},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    slots = data['list']
    print(f"✅ {len(slots)} slots de forecast extraídos (cobertura: {slots[0]['dt_txt']} → {slots[-1]['dt_txt']})")
    return slots


# ---------------------------------------------------------------------------
# Transformação
# ---------------------------------------------------------------------------

def transform_and_merge(**context):
    """Combina o clima atual com cada slot de forecast em uma lista de dicts."""
    ti = context['task_instance']
    current = ti.xcom_pull(task_ids='extract_current')
    slots = ti.xcom_pull(task_ids='extract_forecast')

    captured_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    city = current['name']
    country = current['sys']['country']

    # Dados do clima atual (repetidos em todas as linhas desta run)
    current_data = {
        'captured_at': captured_at,
        'city': city,
        'country': country,
        'current_temp': current['main']['temp'],
        'current_feels_like': current['main']['feels_like'],
        'current_humidity': current['main']['humidity'],
        'current_pressure': current['main']['pressure'],
        'current_wind_speed': current['wind']['speed'],
        'current_description': current['weather'][0]['description'].title(),
    }

    rows = []
    for slot in slots:
        row = {
            **current_data,
            # dt_txt já é uma string legível: "2025-01-01 12:00:00"
            'forecast_for': slot['dt_txt'],
            'forecast_temp': slot['main']['temp'],
            'forecast_feels_like': slot['main']['feels_like'],
            'forecast_humidity': slot['main']['humidity'],
            'forecast_pressure': slot['main']['pressure'],
            'forecast_wind_speed': slot['wind']['speed'],
            'forecast_description': slot['weather'][0]['description'].title(),
        }
        rows.append(row)

    print(f"✅ {len(rows)} linhas montadas para '{city}' ({captured_at})")
    return rows


# ---------------------------------------------------------------------------
# Carga no CSV
# ---------------------------------------------------------------------------

def save_to_csv(**context):
    """Faz append de todas as linhas no CSV forecast_data.csv."""
    ti = context['task_instance']
    rows = ti.xcom_pull(task_ids='transform_and_merge')

    file_exists = os.path.isfile(CSV_PATH)
    print(f"💾 Salvando {len(rows)} linhas em {CSV_PATH}...")

    with open(CSV_PATH, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, '') for field in CSV_FIELDNAMES})

    print(f"✅ CSV atualizado! Total de linhas adicionadas: {len(rows)}")
    return CSV_PATH


# ---------------------------------------------------------------------------
# Sumário
# ---------------------------------------------------------------------------

def summary(**context):
    """Loga um resumo da execução."""
    ti = context['task_instance']
    rows = ti.xcom_pull(task_ids='transform_and_merge')
    csv_path = ti.xcom_pull(task_ids='save_to_csv')

    first = rows[0]
    last = rows[-1]

    print('=' * 60)
    print('ETL WEATHER FORECAST SUMMARY')
    print('=' * 60)
    print(f"🏙️  Cidade      : {first['city']}, {first['country']}")
    print(f"📅 Capturado em : {first['captured_at']}")
    print(f"🌡️  Atual        : {first['current_temp']:.1f}°C — {first['current_description']}")
    print(f"📈 Forecast      : {len(rows)} slots ({first['forecast_for']} → {last['forecast_for']})")
    print(f"💾 CSV           : {csv_path}")
    print('=' * 60)
    print('🎉 ETL Weather Forecast concluído com sucesso!')


# ---------------------------------------------------------------------------
# Definição do DAG
# ---------------------------------------------------------------------------
with DAG(
    'etl_weather_forecast',
    default_args=default_args,
    description=(
        'Coleta clima atual + todos os slots de forecast OWM a cada 3h '
        'e salva em forecast_data.csv para avaliação de acurácia.'
    ),
    schedule='0 */3 * * *',  # 0h, 3h, 6h, 9h, 12h, 15h, 18h, 21h
    catchup=False,
    tags=['etl', 'weather', 'forecast', 'data-collection'],
) as dag:

    t_extract_current = PythonOperator(
        task_id='extract_current',
        python_callable=extract_current,
    )

    t_extract_forecast = PythonOperator(
        task_id='extract_forecast',
        python_callable=extract_forecast,
    )

    t_transform = PythonOperator(
        task_id='transform_and_merge',
        python_callable=transform_and_merge,
    )

    t_save = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
    )

    t_summary = PythonOperator(
        task_id='summary',
        python_callable=summary,
    )

    # extract_current e extract_forecast rodam em paralelo
    [t_extract_current, t_extract_forecast] >> t_transform >> t_save >> t_summary
