# 🚀 Airflow ETL Projects Collection

Este repositório contém uma coleção de projetos ETL (Extract, Transform, Load) implementados com Apache Airflow, demonstrando diferentes casos de uso e padrões de desenvolvimento.

## 📋 Projetos Incluídos

### 1. 🌤️ **ETL Weather Report System**
Sistema de relatórios diários do clima que demonstra a diferença entre script Python simples e implementação Airflow profissional.

**Arquivos:**
- `etl_example.py` - Script Python com Fire para argumentos
- `etl_airflow_example.py` - DAG Airflow com parâmetros na UI
- `etl.md` - Documentação detalhada

**Funcionalidades:**
- ✅ Extração de dados do OpenWeatherMap API
- ✅ Transformação em relatório HTML com emojis
- ✅ Envio por email com design responsivo
- ✅ Comparação Script vs DAG Airflow

### 2. 💬 **Chat Processing Pipeline**
Pipeline completo para processamento de conversas, incluindo pré-processamento de dados e integração com API externa.

**Arquivos:**
- `prep_chats.py` - DAG para pré-processamento de CSVs
- `clair_api_processor.py` - DAG para processamento via API
- `test_email_connection.py` - DAG de teste para configuração de email

**Funcionalidades:**
- ✅ Processamento de múltiplos arquivos CSV
- ✅ Adição de colunas (dialog_id, username, timestamp)
- ✅ Remoção de duplicatas consecutivas
- ✅ Geração de Excel com múltiplas planilhas
- ✅ Integração com API externa (Clair)
- ✅ Envio automático de resultados por email

## 🛠️ Instalação e Configuração

### Pré-requisitos
- Python 3.10+
- Conda ou Miniconda
- Conta Gmail com senha de aplicativo
- API Key do OpenWeatherMap (para ETL Weather)

### 1. Configuração do Ambiente Base

```bash
# Criar ambiente conda
conda create -n airflow-test python=3.10
conda activate airflow-test

# Instalar dependências
pip install -r requirements.txt

# Configurar Airflow
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 2. Configuração do Ambiente Sensível (clair-py310)

⚠️ **IMPORTANTE**: Este ambiente contém dependências sensíveis e deve ser tratado com cuidado.

```bash
# Criar cópia do ambiente sensível
conda create -n clair-py310-copy --clone clair-py310

# Ou criar novo ambiente com dependências específicas
conda create -n clair-py310-copy python=3.10
conda activate clair-py310-copy

# Instalar dependências essenciais
pip install tensorflow==2.19.1
pip install keras==3.11.3
pip install pandas==2.3.2
pip install numpy==2.0.1
pip install requests==2.32.5
pip install openpyxl==3.1.5
pip install tqdm==4.67.1
pip install fire==0.7.1
```

### 3. Configuração de Email

```bash
# Configurar variáveis de ambiente
export SENDER_EMAIL="seu_email@gmail.com"
export SENDER_PASSWORD="sua_senha_de_app_gmail"

# Configurar conexão SMTP no Airflow
# Admin → Connections → Add Connection
# Connection Id: smtp_default
# Connection Type: Email
# Host: smtp.gmail.com
# Login: seu_email@gmail.com
# Password: sua_senha_de_app
# Port: 587
# Extra: {"smtp_starttls": true, "smtp_ssl": false}
```

### 4. Configuração de APIs

```bash
# OpenWeatherMap API (para ETL Weather)
export OPENWEATHER_API_KEY="sua_api_key_aqui"

# Clair API (para Chat Processing)
export CLAIR_URL="https://next-lab-test.bms.utwente.nl/chatBot/"
export CLAIR_TOKEN="seu_token_aqui"
```

## 🚀 Como Executar

### ETL Weather Report

#### Script Python Simples:
```bash
# Ativar ambiente
conda activate airflow-test

# Executar com parâmetros
python etl_example.py --city "São Paulo" --email "destinatario@email.com" --api_key "sua_api_key"
```

#### DAG Airflow:
1. Iniciar Airflow: `airflow standalone`
2. Acessar UI: http://localhost:8080
3. Configurar parâmetros do DAG `etl_weather_report`
4. Executar manualmente ou aguardar agendamento (8h da manhã)

### Chat Processing Pipeline

#### 1. Pré-processamento (prep_chats):
1. Configurar parâmetro `data_folder` na UI
2. Executar DAG `prep_chats`
3. Aguardar geração dos arquivos processados

#### 2. Processamento via API (clair_api_processor):
1. Configurar parâmetros na UI:
   - `data_file_path`: Caminho do CSV combinado
   - `recipient_email`: Email para receber resultados
2. Executar DAG `clair_api_processor`
3. Aguardar processamento e receber email com Excel

## 📊 Estrutura do Projeto

```
airflow-etl-projects/
├── dags/
│   ├── etl_example.py              # Script Python ETL Weather
│   ├── etl_airflow_example.py      # DAG Airflow ETL Weather
│   ├── prep_chats.py               # DAG Pré-processamento Chats
│   ├── clair_api_processor.py      # DAG Processamento API
│   ├── test_email_connection.py    # DAG Teste Email
│   └── test_email_*.py            # DAGs de teste adicionais
├── requirements.txt                # Dependências Python
├── etl.md                         # Documentação ETL Weather
└── README.md                      # Este arquivo
```

## 🔧 Configurações Avançadas

### Personalização de Agendamento

```python
# No DAG, modifique a linha schedule:
schedule='0 8 * * *'    # Diário às 8h
schedule='0 */6 * * *'  # A cada 6 horas
schedule='0 9 * * 1'    # Segundas-feiras às 9h
```

### Configuração de Retry

```python
# No DAG, modifique default_args:
default_args = {
    'retries': 3,  # Número de tentativas
    'retry_delay': timedelta(minutes=10),  # Intervalo entre tentativas
}
```

### Variáveis de Ambiente Adicionais

```bash
# Para desenvolvimento
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/your/dags
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Para produção
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CELERY__BROKER_URL=redis://localhost:6379/0
```

## 🐛 Troubleshooting

### Problemas Comuns

#### 1. Erro de Conexão SMTP
```
SSLError: [SSL: WRONG_VERSION_NUMBER]
```
**Solução**: Verificar configuração da conexão SMTP e usar senha de aplicativo Gmail.

#### 2. Erro de Dependências
```
ModuleNotFoundError: No module named 'openpyxl'
```
**Solução**: Instalar dependências: `pip install openpyxl`

#### 3. Erro de API
```
requests.exceptions.RequestException
```
**Solução**: Verificar API key e conectividade de rede.

#### 4. Erro de Permissões
```
PermissionError: [Errno 13] Permission denied
```
**Solução**: Verificar permissões de arquivo e diretório.

### Logs e Debugging

```bash
# Ver logs do Airflow
tail -f ~/airflow/logs/scheduler/latest/scheduler.log

# Ver logs de um DAG específico
ls ~/airflow/logs/dag_id=etl_weather_report/

# Debug de conexões
airflow connections list
airflow connections get smtp_default
```

## 📈 Monitoramento e Métricas

### Airflow UI
- **DAGs**: Visualização de todos os DAGs e status
- **Tasks**: Detalhes de execução de cada task
- **Logs**: Logs em tempo real de cada execução
- **Connections**: Gerenciamento de conexões
- **Variables**: Variáveis de configuração

### Métricas Importantes
- **Taxa de Sucesso**: % de execuções bem-sucedidas
- **Tempo de Execução**: Duração média dos DAGs
- **Retry Rate**: Frequência de tentativas
- **Resource Usage**: Uso de CPU/memória

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 🙏 Agradecimentos

- [Apache Airflow](https://airflow.apache.org/) - Plataforma de orquestração
- [OpenWeatherMap](https://openweathermap.org/) - API de dados climáticos
- [Google](https://developers.google.com/) - Python Fire library
- Comunidade Python e Apache Airflow

## 📞 Suporte

Para dúvidas e suporte:
- Abra uma [Issue](../../issues) no GitHub
- Consulte a [documentação do Airflow](https://airflow.apache.org/docs/)
- Participe da [comunidade Airflow](https://airflow.apache.org/community/)

---

**Desenvolvido com ❤️ para demonstrar o poder do Apache Airflow em processos ETL!**
