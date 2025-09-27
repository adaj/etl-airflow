# 🚀 Airflow DAGs Collection

Este repositório contém uma coleção de DAGs do Apache Airflow para diferentes casos de uso de ETL e processamento de dados.

## 📁 Estrutura do Projeto

### 🌤️ [ETL Weather](./etl-weather/)
Projetos relacionados ao processamento de dados meteorológicos:
- **`etl_example.py`** - Script standalone para ETL de dados meteorológicos usando Fire
- **`etl_airflow_example.py`** - DAG do Airflow para o mesmo processo ETL
- **`README.md`** - Documentação detalhada dos exemplos ETL

### 💬 [Chat Processing](./chat-processing/)
Projetos para processamento e análise de conversas:
- **`prep_chats.py`** - DAG para processar arquivos CSV de conversas
- **`clair_api_processor.py`** - DAG para enviar dados para API Clair e enviar resultados por email
- **`test_email_connection.py`** - DAG de teste para configuração de email

## 🛠️ Configuração

### Pré-requisitos
- Python 3.10
- Apache Airflow 3.x
- Conda (recomendado para gerenciamento de ambientes)

### Instalação
```bash
# Clone o repositório
git clone https://github.com/adaj/etl-airflow.git
cd etl-airflow

# Instale as dependências
pip install -r requirements.txt
```

### Configuração do Ambiente
1. **Crie um ambiente conda:**
   ```bash
   conda create -n airflow-test python=3.10
   conda activate airflow-test
   ```

2. **Instale o Airflow:**
   ```bash
   pip install apache-airflow
   pip install openpyxl  # Para suporte a Excel
   ```

3. **Configure o Airflow:**
   ```bash
   airflow db init
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```

## 🚀 Executando os DAGs

### Airflow Standalone
```bash
airflow standalone
```

### Acesse a Interface
- **URL:** http://localhost:8080
- **Usuário:** admin
- **Senha:** (gerada automaticamente no terminal)

## 📋 Configurações Necessárias

### Conexões do Airflow
Configure as seguintes conexões na interface do Airflow (Admin > Connections):

1. **SMTP (para envio de emails):**
   - Connection ID: `smtp_default`
   - Connection Type: `Email`
   - Host: `smtp.gmail.com`
   - Port: `587`
   - Login: `seu-email@gmail.com`
   - Password: `sua-app-password`
   - Extra: `{"smtp_starttls": true, "smtp_ssl": false}`

## 🔧 Troubleshooting

### Problemas Comuns
1. **Erro de dependências:** Use `pip install -r requirements.txt`
2. **Problemas de email:** Verifique as configurações SMTP e use App Password do Gmail
3. **Erro de ambiente:** Certifique-se de usar Python 3.10

### Logs
- Logs do Airflow: `~/airflow/logs/`
- Logs específicos de cada DAG: Interface do Airflow > DAGs > [Nome do DAG] > Logs

## 📚 Documentação Adicional

- [ETL Weather Documentation](./etl-weather/README.md)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Conda Documentation](https://docs.conda.io/)

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 👨‍💻 Autor

**Adelson D. de Araujo Jr**
- GitHub: [@adaj](https://github.com/adaj)
- Email: adelson.dias@gmail.com

---

⭐ **Se este projeto foi útil para você, considere dar uma estrela!**
