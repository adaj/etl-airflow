# Coleção de exemplos de gerenciamento de workflows com Apache Airflow

Este repositório reúne exemplos práticos de DAGs para quem está aprendendo a usar o Apache Airflow. Os exemplos foram pensados para ilustrar conceitos como extração de dados de APIs, transformações, carregamento em arquivos e envio de notificações por email.

## Estrutura

```
dags/
└── etl-weather/
    ├── etl_weather_report.py    # DAG de relatório diário do clima por email
    └── etl_weather_forecast.py  # DAG de coleta contínua para avaliação de forecast
```

## 1. Instalação do Airflow

```bash
# Criar ambiente conda
conda create -n airflow-env python=3.10
conda activate airflow-env

# Instalar Apache Airflow
pip install apache-airflow==3.0.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt"

# Iniciar Airflow standalone
airflow standalone
```

## 2. Acessar a Interface

- **URL:** http://localhost:8080
- **Usuário:** admin
- **Senha:** Consulte `~/airflow/simple_auth_manager_passwords.json.generated`

## 3. Executar DAGs

1. Acesse a interface do Airflow
2. Encontre os DAGs disponíveis
3. Configure os parâmetros necessários (ver seção abaixo)
4. Execute manualmente ou aguarde o agendamento

## ⚙️ Configurações no Airflow

### Variáveis (Admin > Variables)

Os DAGs leem configurações sensíveis de Airflow Variables — nunca do código. Isso evita expor chaves e facilita mudanças sem alterar o script.

| Variable | Descrição | Usado em |
|---|---|---|
| `owm_api_key` | Chave da API OpenWeatherMap | Ambos os DAGs |
| `owm_target_city` | Cidade-alvo da coleta (ex: `Natal`) | `etl_weather_forecast` |

### Conexões (Admin > Connections)

Para o envio de email, configure a conexão SMTP:

- **Connection ID:** `smtp_default`
- **Connection Type:** `Email`
- **Host:** `smtp.gmail.com`
- **Port:** `587`
- **Login:** `seu-email@gmail.com`
- **Password:** sua App Password do Gmail *(não a senha da conta — gere em myaccount.google.com/apppasswords)*

## 📋 DAGs disponíveis

### `etl_weather_report`
Roda diariamente às 8h. Consulta o clima atual de uma cidade, gera um relatório HTML e envia por email. A cidade é configurada como parâmetro no trigger. Salva um histórico em `weather_history.csv`.

### `etl_weather_forecast`
Roda a cada 3 horas. Coleta o clima atual e todos os slots de previsão dos próximos 5 dias (endpoint `/forecast` da OWM, ~40 entradas por run). Salva tudo em `forecast_data.csv` para possibilitar análise de acurácia do forecast ao longo do tempo. A cidade é lida da Variable `owm_target_city`.
