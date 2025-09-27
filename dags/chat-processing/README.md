# 💬 Chat Processing DAGs

Esta pasta contém DAGs do Airflow para processamento e análise de conversas/chat.

## 📋 DAGs Disponíveis

### 1. `prep_chats.py` - Preparação de Dados de Chat
**Descrição:** Processa múltiplos arquivos CSV de conversas e gera saídas organizadas.

**Funcionalidades:**
- ✅ Descoberta automática de arquivos CSV em uma pasta
- ✅ Processamento individual de cada arquivo
- ✅ Adição de colunas: `dialog_id` (nome do arquivo) e `username` (SpeakerA/SpeakerB aleatório)
- ✅ Conversão de timestamps para formato pandas
- ✅ Remoção de linhas duplicadas consecutivas
- ✅ Geração de arquivo Excel com múltiplas planilhas
- ✅ Geração de arquivo CSV combinado
- ✅ Estatísticas detalhadas do processamento

**Parâmetros (configuráveis na UI):**
- `data_folder_path`: Caminho da pasta com arquivos CSV (padrão: `/Users/adelson/Documents/mai-thomas/VR_WC_Transcripts`)

**Saídas:**
- `data/processed_chats.xlsx` - Arquivo Excel com uma planilha por diálogo
- `data/combined_chats.csv` - Arquivo CSV combinado com todos os diálogos

### 2. `clair_api_processor.py` - Processamento via API Clair
**Descrição:** Envia dados processados para a API Clair e envia resultados por email.

**Funcionalidades:**
- ✅ Processamento de dados via API Clair
- ✅ Geração de arquivo Excel com resultados
- ✅ Envio automático de resultados por email
- ✅ Validação de arquivos de saída
- ✅ Logs detalhados com progresso em tempo real

**Parâmetros (configuráveis na UI):**
- `data_file_path`: Caminho do arquivo CSV de entrada (padrão: `/Users/adelson/Documents/mai-thomas/data/combined_chats.csv`)
- `dataset_lang`: Idioma do dataset (padrão: `EN`)
- `topics_file`: Tópicos para análise (padrão: `default, artificial intelligence`)
- `mode`: Modo de processamento (padrão: `ssrl`)
- `n_groups`: Número de grupos (padrão: `-1` para todos)
- `recipient_email`: Email do destinatário (padrão: `adelson.dias@gmail.com`)

**Variáveis de Ambiente:**
- `CLAIR_URL`: URL da API Clair
- `CLAIR_TOKEN`: Token de autenticação

**Saídas:**
- `data/clair_results.xlsx` - Resultados da análise da API
- Email com arquivo anexado

### 3. `test_email_connection.py` - Teste de Conexão de Email
**Descrição:** DAG simples para testar a configuração de email.

**Funcionalidades:**
- ✅ Teste de conexão SMTP
- ✅ Envio de email de teste
- ✅ Validação de configurações

## 🔧 Configuração Necessária

### Conexões do Airflow
Configure a conexão SMTP na interface do Airflow (Admin > Connections):

**SMTP Connection:**
- Connection ID: `smtp_default`
- Connection Type: `Email`
- Host: `smtp.gmail.com`
- Port: `587`
- Login: `seu-email@gmail.com`
- Password: `sua-app-password` (use App Password do Gmail)
- Extra: `{"smtp_starttls": true, "smtp_ssl": false}`

### Variáveis de Ambiente
Para o `clair_api_processor.py`, configure as seguintes variáveis:
- `CLAIR_URL`: `https://next-lab-test.bms.utwente.nl/chatBot/`
- `CLAIR_TOKEN`: `ZMhoyTJQxNE`

## 🚀 Como Executar

### 1. Preparação de Dados
1. Execute `prep_chats.py` primeiro
2. Configure o parâmetro `data_folder_path` com a pasta contendo seus CSVs
3. Aguarde a conclusão para gerar os arquivos processados

### 2. Processamento via API
1. Execute `clair_api_processor.py`
2. Configure os parâmetros conforme necessário
3. Aguarde o processamento e receba o email com resultados

### 3. Teste de Email
1. Execute `test_email_connection.py` para validar configurações
2. Verifique se o email de teste foi recebido

## 📊 Fluxo de Dados

```
CSV Files → prep_chats.py → combined_chats.csv → clair_api_processor.py → Email + Excel
```

## 🔍 Troubleshooting

### Problemas Comuns

1. **Erro de conexão SMTP:**
   - Verifique se está usando App Password do Gmail
   - Confirme as configurações de conexão no Airflow

2. **Arquivo não encontrado:**
   - Verifique os caminhos dos parâmetros
   - Certifique-se de que os arquivos existem

3. **Erro na API Clair:**
   - Verifique as variáveis de ambiente
   - Confirme se a API está acessível

4. **Progress bar não aparece em tempo real:**
   - Isso é uma limitação do Airflow
   - Os logs aparecem após a conclusão da tarefa

## 📈 Monitoramento

- **Logs:** Interface do Airflow > DAGs > [Nome do DAG] > Logs
- **Status:** Interface do Airflow > DAGs > [Nome do DAG] > Graph/Tree View
- **Histórico:** Interface do Airflow > DAGs > [Nome do DAG] > Runs

## 🔗 Dependências

- `pandas` - Manipulação de dados
- `openpyxl` - Suporte a Excel
- `requests` - Chamadas HTTP para API
- `tqdm` - Barras de progresso
- `smtplib` - Envio de emails
