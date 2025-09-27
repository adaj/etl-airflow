# ETL Weather Report System

Este projeto demonstra a implementação de um sistema ETL (Extract, Transform, Load) para relatórios diários do clima, comparando duas abordagens:

1. **Script Python Simples** (`etl_example.py`) - Execução manual com argumentos
2. **DAG Apache Airflow** (`etl_airflow_example.py`) - Orquestração automatizada

## 🌤️ Funcionalidades

- **Extração**: Busca dados do clima em tempo real via OpenWeatherMap API
- **Transformação**: Converte dados brutos em formato legível
- **Carregamento**: Envia relatório HTML por email com emojis e formatação
- **Monitoramento**: Logs detalhados e tratamento de erros

## 📋 Pré-requisitos

### Para ambos os scripts:
- Python 3.8+
- Conta no [OpenWeatherMap](https://openweathermap.org/api) (gratuita)
- Conta Gmail com senha de aplicativo configurada

### Para o script Python:
```bash
pip install requests fire python-dotenv
```

### Para o DAG Airflow:
- Apache Airflow instalado e configurado
- Conexão SMTP configurada (`smtp_default`)

## 🚀 Como Executar

### 1. Script Python Simples (`etl_example.py`)

#### Configuração de Variáveis de Ambiente:
```bash
# API Key do OpenWeatherMap
export OPENWEATHER_API_KEY="sua_api_key_aqui"

# Credenciais de email (opcional, pode usar argumentos)
export SENDER_EMAIL="seu_email@gmail.com"
export SENDER_PASSWORD="sua_senha_de_app"
```

#### Execução:
```bash
# Execução básica
python etl_example.py

# Com parâmetros personalizados
python etl_example.py --city "Rio de Janeiro" --email "destinatario@email.com" --api_key "sua_api_key"

# Ajuda
python etl_example.py --help
```

#### Exemplo de Saída:
```
🚀 Iniciando processo ETL...
==================================================
🌤️  Extraindo dados do clima para São Paulo...
✅ Dados extraídos com sucesso!
🔄 Transformando dados...
✅ Dados transformados com sucesso!
📝 Gerando relatório HTML...
✅ Relatório HTML gerado!
📧 Enviando email para adelson.dias@gmail.com...
✅ Email enviado com sucesso!
==================================================
🎉 Processo ETL concluído com sucesso!
```

### 2. DAG Apache Airflow (`etl_airflow_example.py`)

#### Configuração:
1. **API Key**: Configure no parâmetro `api_key` do DAG
2. **Conexão SMTP**: Certifique-se que `smtp_default` está configurada
3. **Parâmetros**: Ajuste cidade e email de destino na UI

#### Execução:
1. Acesse a interface do Airflow
2. Encontre o DAG `etl_weather_report`
3. Configure os parâmetros:
   - `city`: Nome da cidade (ex: "São Paulo")
   - `api_key`: Sua chave da API OpenWeatherMap
   - `recipient_email`: Email de destino
4. Execute manualmente ou aguarde o agendamento (8h da manhã)

#### Monitoramento:
- **Logs em tempo real** de cada task
- **Retry automático** em caso de falha
- **Notificações por email** em falhas
- **Histórico de execuções**

## 🔄 Comparação das Abordagens

| Aspecto | Script Python | DAG Airflow |
|---------|---------------|-------------|
| **Execução** | Manual via linha de comando | Automatizada e agendada |
| **Monitoramento** | Logs no terminal | Interface web completa |
| **Retry** | Manual | Automático (2 tentativas) |
| **Parâmetros** | Argumentos de linha de comando | Interface web intuitiva |
| **Agendamento** | Cron manual | Integrado ao Airflow |
| **Notificações** | Apenas sucesso | Sucesso e falhas |
| **Escalabilidade** | Limitada | Alta |
| **Manutenção** | Manual | Centralizada |

## 📊 Estrutura do Processo ETL

### Extract (Extração)
```python
# Busca dados da API OpenWeatherMap
response = requests.get(api_url, params=params)
weather_data = response.json()
```

### Transform (Transformação)
```python
# Converte dados brutos em formato estruturado
transformed = {
    'city': data['name'],
    'temperature': data['main']['temp'],
    'humidity': data['main']['humidity'],
    # ... outros campos
}
```

### Load (Carregamento)
```python
# Gera HTML e envia por email
html_report = generate_html_report(transformed_data)
send_email(html_report)
```

## 🎨 Relatório HTML

O sistema gera relatórios HTML responsivos com:

- **Emojis dinâmicos** baseados nas condições climáticas
- **Design responsivo** para mobile e desktop
- **Métricas organizadas** em formato de tabela
- **Branding do Airflow** (apenas no DAG)
- **Informações de execução** (timestamp, data)

### Exemplo de Métricas:
- 🌡️ Temperatura atual
- 🤔 Sensação térmica
- 💧 Umidade relativa
- 📊 Pressão atmosférica
- 🌬️ Velocidade do vento
- ☁️ Condições climáticas

## 🔧 Configuração Avançada

### Personalização do Agendamento (Airflow)
```python
# No DAG, modifique a linha:
schedule='0 8 * * *'  # Diário às 8h
# Para outras opções:
schedule='0 */6 * * *'  # A cada 6 horas
schedule='0 9 * * 1'    # Segundas-feiras às 9h
```

### Adicionando Novas Cidades
```python
# No script Python:
python etl_example.py --city "Nova York" --city "Londres"

# No DAG Airflow:
# Modifique o parâmetro 'city' na UI
```

### Configuração de Retry
```python
# No DAG Airflow:
default_args = {
    'retries': 3,  # Número de tentativas
    'retry_delay': timedelta(minutes=10),  # Intervalo entre tentativas
}
```

## 🚨 Tratamento de Erros

### Script Python:
- Validação de API key
- Timeout de requisições (10s)
- Verificação de campos obrigatórios
- Logs detalhados de erro

### DAG Airflow:
- Retry automático (2 tentativas)
- Notificações por email em falhas
- Logs centralizados
- Rollback automático

## 📈 Benefícios do Airflow

1. **Orquestração**: Gerencia dependências entre tasks
2. **Monitoramento**: Interface web para acompanhar execuções
3. **Escalabilidade**: Suporte a múltiplos workers
4. **Confiabilidade**: Retry automático e tratamento de falhas
5. **Flexibilidade**: Parâmetros configuráveis via UI
6. **Integração**: Conecta com bancos de dados, APIs, etc.
7. **Agendamento**: Cron integrado para automação

## 🔗 Links Úteis

- [OpenWeatherMap API](https://openweathermap.org/api)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Python Fire Library](https://github.com/google/python-fire)
- [Gmail App Passwords](https://support.google.com/accounts/answer/185833)

## 📝 Próximos Passos

Para expandir este sistema, considere:

1. **Múltiplas cidades**: Processar várias cidades em paralelo
2. **Histórico**: Armazenar dados em banco de dados
3. **Alertas**: Notificações para condições extremas
4. **Dashboard**: Interface web para visualizar dados
5. **Integração**: Conectar com outros serviços (Slack, Teams)

---

**Desenvolvido com ❤️ para demonstrar o poder do Apache Airflow em processos ETL!**
