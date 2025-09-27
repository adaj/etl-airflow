# 🚀 Airflow ETL Projects Collection

Uma coleção de projetos ETL (Extract, Transform, Load) implementados com Apache Airflow, demonstrando diferentes casos de uso e padrões de desenvolvimento.

## 📁 Estrutura do Projeto

```
dags/
├── etl-weather/                 # 🌤️ Projetos ETL de clima
│   ├── README.md               # 📖 Documentação específica
│   ├── etl_example.py          # 🔥 Script standalone (Fire)
│   └── etl_airflow_example.py  # 🚀 DAG do Airflow
└── chat-processing/             # 💬 Processamento de chat
    ├── README.md               # 📖 Documentação específica
    ├── clair_api_processor.py  # 🔗 API Clair + Email
    ├── prep_chats.py           # 📊 Preparação de dados
    └── test_email_connection.py # 📧 Teste de email
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

### 💬 **Chat Processing Pipeline**
- **Pré-processamento:** `dags/chat-processing/prep_chats.py`
- **API Processing:** `dags/chat-processing/clair_api_processor.py`
- **Teste Email:** `dags/chat-processing/test_email_connection.py`
- **Funcionalidade:** Processamento de conversas e integração com API externa

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

### Variáveis de Ambiente
```bash
# Para ETL Weather
export OPENWEATHER_API_KEY="sua_api_key"

# Para Chat Processing
export CLAIR_URL="url_da_api_clair"
export CLAIR_TOKEN="seu_token"
```

## 📚 Documentação Detalhada

- [ETL Weather Documentation](./dags/etl-weather/README.md)
- [Chat Processing Documentation](./dags/chat-processing/README.md)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

## 🔧 Troubleshooting

### Problemas Comuns
1. **Erro de dependências:** `pip install -r requirements.txt`
2. **Problemas de email:** Use App Password do Gmail
3. **Erro de ambiente:** Certifique-se de usar Python 3.10

### Logs
- Logs do Airflow: `~/airflow/logs/`
- Logs específicos: Interface do Airflow > DAGs > [Nome do DAG] > Logs

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT.

---

**Desenvolvido com ❤️ para demonstrar o poder do Apache Airflow em processos ETL!**