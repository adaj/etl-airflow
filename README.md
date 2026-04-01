# 🚀 Airflow ETL Projects Collection

Uma coleção de projetos ETL (Extract, Transform, Load) implementados com Apache Airflow, demonstrando diferentes casos de uso e padrões de desenvolvimento.

## 📁 Estrutura do Projeto

```
dags/
├── etl-weather/                  Projetos ETL de clima
│   ├── README.md                Documentação específica
│   ├── etl_example.py           Script standalone
│   └── etl_airflow_example.py   
```

## 🚀 Quick Start

### 1. Instalação do Airflow

```bash
# Criar ambiente conda
conda create -n airflow-env python=3.10
conda activate airflow-env

# Instalar Apache Airflow
pip install apache-airflow==3.0.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt"

# Iniciar Airflow standalone
airflow standalone
```

### 2. Acessar a Interface

- **URL:** http://localhost:8080
- **Usuário:** admin
- **Senha:** Consulte `~/airflow/simple_auth_manager_passwords.json.generated`

### 3. Executar DAGs

1. Acesse a interface do Airflow
2. Encontre os DAGs disponíveis
3. Configure os parâmetros necessários
4. Execute manualmente ou aguarde o agendamento

## 📋 Projetos Disponíveis

### 🌤️ **ETL Weather Report**
- **Script Python:** `dags/etl-weather/etl_example.py`
- **DAG Airflow:** `dags/etl-weather/etl_airflow_example.py`
- **Funcionalidade:** Relatórios diários do clima via OpenWeatherMap API


## ⚙️ Configuração Necessária

### Conexões do Airflow

Configure na interface (Admin > Connections):

**SMTP Connection:**
- Connection ID: `smtp_default`
- Connection Type: `Email`
- Host: `smtp.gmail.com`
- Port: `587`
- Login: `seu-email@gmail.com`
- Password: `sua-app-password`
- Extra: `{"smtp_starttls": true, "smtp_ssl": false}`

Configure na interface (Admin > Variables):

**Variables**
- `owm_api_key`: Sua chave da API OpenWeatherMap.
- `owm_city`: Cidade para extrair os dados do clima.

