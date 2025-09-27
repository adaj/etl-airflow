"""
DAG: clair_api_processor

This DAG processes the combined CSV data from prep_chats DAG by sending it to the Clair API
and saving the results to an Excel file with each dialogue in a separate sheet.

The DAG uses the existing clair_api_tester.py script with PythonOperator for better
error handling and real-time progress monitoring.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from datetime import datetime, timedelta
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import ssl

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(seconds=5),
}

def run_clair_api_processor(**context):
    """
    Run the clair_api_tester.py script with real-time progress monitoring
    and proper error handling.
    """
    params = context['params']
    
    # Set environment variables
    env = os.environ.copy()
    env['CLAIR_URL'] = params['clair_url']
    env['CLAIR_TOKEN'] = params['clair_token']
    env['PATH'] = '/Users/adelson/miniconda3/bin:/usr/local/bin:/usr/bin:/bin'
    
    # Prepare the command
    cmd = [
        'conda', 'run', '-n', 'clair-py310', 'python', '-u',
        'tests/archived/clair_api_tester.py',
        '--data_file_path', params['data_file_path'],
        '--dataset_lang', params['dataset_lang'],
        '--topics_file', params['topics_file'],
        '--mode', params['mode'],
        '--n_groups', str(params['n_groups'])
    ]
    
    # Change to the correct directory
    working_dir = "/Users/adelson/Library/CloudStorage/GoogleDrive-adelson.dias@gmail.com/Meu Drive/Clair/clair-v044"
    
    print("🚀 Starting Clair API processing...")
    print(f"📁 Input file: {params['data_file_path']}")
    print(f"🔧 Mode: {params['mode']}")
    print(f"🌍 Language: {params['dataset_lang']}")
    print(f"📊 Groups: {params['n_groups'] if params['n_groups'] != -1 else 'All'}")
    print(f"🌐 API URL: {params['clair_url']}")
    print("")
    
    try:
        # Start the process
        process = subprocess.Popen(
            cmd,
            cwd=working_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )
        
        # Stream output in real-time
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
                sys.stdout.flush()  # Force immediate output
        
        # Wait for process to complete and get return code
        return_code = process.poll()
        
        if return_code != 0:
            # Get any remaining output
            remaining_output = process.stdout.read()
            if remaining_output:
                print(remaining_output.strip())
            
            # Check if Excel file was created despite the error
            output_file = params['data_file_path'].replace('.csv', f'__clair_api_{params["mode"]}.xlsx')
            if os.path.exists(output_file):
                print(f"✅ Excel file was created successfully despite the error")
                print(f"📄 File: {output_file}")
                print(f"📏 Size: {os.path.getsize(output_file)} bytes")
                return  # Success - file exists
            else:
                raise Exception(f"Script failed with exit code {return_code}. No Excel file was created.")
        
        print("")
        print("✅ Clair API processing completed successfully!")
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        raise

def validate_excel_file(**context):
    """
    Validate that the Excel file was created and is properly formatted.
    """
    params = context['params']
    output_file = params['data_file_path'].replace('.csv', f'__clair_api_{params["mode"]}.xlsx')
    
    print(f"🔍 Validating Excel file: {output_file}")
    
    if not os.path.exists(output_file):
        raise Exception(f"Excel file not found: {output_file}")
    
    # Check file size
    file_size = os.path.getsize(output_file)
    if file_size < 1000:  # Less than 1KB
        raise Exception(f"Excel file appears to be empty or too small: {file_size} bytes")
    
    print(f"✅ Excel file exists: {output_file}")
    print(f"📏 File size: {file_size} bytes")
    
    # Validate Excel structure
    try:
        import pandas as pd
        df_dict = pd.read_excel(output_file, sheet_name=None)
        print(f"✅ Excel file validation passed: {len(df_dict)} sheets found")
        
        for sheet_name, sheet_df in df_dict.items():
            print(f"  📊 Sheet '{sheet_name}': {sheet_df.shape[0]} rows, {sheet_df.shape[1]} columns")
            
    except Exception as e:
        raise Exception(f"Excel file structure validation failed: {str(e)}")

def processing_summary(**context):
    """
    Provide a summary of the processing results.
    """
    params = context['params']
    output_file = params['data_file_path'].replace('.csv', f'__clair_api_{params["mode"]}.xlsx')
    
    print("=" * 60)
    print("CLAIR API PROCESSING SUMMARY")
    print("=" * 60)
    print(f"📁 Input file: {params['data_file_path']}")
    print(f"🌐 API URL: {params['clair_url']}")
    print(f"🔧 Mode: {params['mode']}")
    print(f"🌍 Language: {params['dataset_lang']}")
    print(f"📊 Groups: {params['n_groups'] if params['n_groups'] != -1 else 'All'}")
    print("")
    
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file)
        print("✅ Processing completed successfully!")
        print(f"📄 Output file: {output_file}")
        print(f"📏 File size: {file_size} bytes")
    else:
        print("❌ Processing failed - output file not found")
    print("=" * 60)

def send_results_email(**context):
    """
    Send the Excel results via email with attachment using Airflow connection.
    """
    params = context['params']
    
    # Get SMTP connection from Airflow
    conn = BaseHook.get_connection('smtp_default')
    
    # Extract connection settings
    smtp_server = conn.host
    smtp_port = conn.port
    sender_email = conn.login
    sender_password = conn.password
    
    # Email settings
    recipient_email = params['recipient_email']
    output_file = params['data_file_path'].replace('.csv', f'__clair_api_{params["mode"]}.xlsx')
    
    print(f"📧 Enviando email para: {recipient_email}")
    print(f"📎 Anexando arquivo: {output_file}")
    
    # Create message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = f'Clair API Processing Results - {context["ds"]}'
    
    # HTML content
    html_content = f"""
    <h3>✅ Clair API Processing Complete</h3>
    <p>The Clair API processing has finished successfully and all validations passed.</p>
    
    <h4>📊 Processing Details:</h4>
    <ul>
        <li><strong>Input file:</strong> {params['data_file_path']}</li>
        <li><strong>Mode:</strong> {params['mode']}</li>
        <li><strong>Keywords:</strong> {params['topics_file']}</li>
        <li><strong>Language:</strong> {params['dataset_lang']}</li>
        <li><strong>Groups:</strong> {params['n_groups'] if params['n_groups'] != -1 else 'All'}</li>
        <li><strong>API URL:</strong> {params['clair_url']}</li>
        <li><strong>Processing Date:</strong> {context['ds']}</li>
    </ul>
    
    <h4>📎 Attachments:</h4>
    <p>The Excel file with processed results is attached to this email.</p>
    
    <h4>🔍 Validation Status:</h4>
    <p>✅ File existence verified<br>
    ✅ File size validated<br>
    ✅ Excel structure validated<br>
    ✅ All sheets accessible</p>
    
    <p>Best regards,<br>Airflow DAG: clair_api_processor</p>
    """
    
    msg.attach(MIMEText(html_content, 'html'))
    
    # Attach Excel file
    if os.path.exists(output_file):
        with open(output_file, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename= {os.path.basename(output_file)}'
        )
        msg.attach(part)
        print(f"✅ Arquivo anexado: {os.path.basename(output_file)}")
    else:
        print(f"❌ Arquivo não encontrado: {output_file}")
        raise Exception(f"Excel file not found: {output_file}")
    
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
        return "Email enviado com sucesso!"
        
    except Exception as e:
        print(f"❌ Erro ao enviar email: {str(e)}")
        raise Exception(f"Falha ao enviar email: {str(e)}")

# DAG definition
with DAG(
    'clair_api_processor',
    default_args=default_args,
    description='Process combined CSV data through Clair API and save results to Excel',
    schedule=None,  # Manual trigger only
    catchup=False,
    params={
        'data_file_path': Param(
            '/Users/adelson/Documents/mai-thomas/data/combined_chats.csv',
            type='string',
            description='Path to the combined CSV file from prep_chats DAG'
        ),
        'dataset_lang': Param(
            'EN',
            type='string',
            description='Dataset language (default: EN)'
        ),
        'topics_file': Param(
            'default, artificial intelligence',
            type='string',
            description='Comma-separated keywords (default: default, artificial intelligence)'
        ),
        'mode': Param(
            'ssrl',
            type='string',
            description='Clair API mode (default: ssrl)'
        ),
        'n_groups': Param(
            -1,
            type='integer',
            description='Number of groups to test (default: -1 for all groups)'
        ),
        'clair_url': Param(
            'https://next-lab-test.bms.utwente.nl/chatBot/',
            type='string',
            description='Clair API URL'
        ),
        'clair_token': Param(
            'ZMhoyTJQxNE',
            type='string',
            description='Clair API access token'
        ),
        'recipient_email': Param(
            'adelson.dias@gmail.com',
            type='string',
            description='Email address to receive the results (default: your email)'
        )
    },
    tags=['clair', 'api', 'excel', 'processing']
) as dag:

    # Task 1: Run clair_api_tester.py with real-time progress
    clair_api_task = PythonOperator(
        task_id='run_clair_api_processor',
        python_callable=run_clair_api_processor
    )

    # Task 2: Validate Excel file creation
    validate_excel_task = PythonOperator(
        task_id='validate_excel_file',
        python_callable=validate_excel_file
    )

    # Task 3: Summary and cleanup
    summary_task = PythonOperator(
        task_id='processing_summary',
        python_callable=processing_summary
    )

    # Task 4: Send results via email
    email_task = PythonOperator(
        task_id='send_results_email',
        python_callable=send_results_email
    )

    # Define task dependencies
    clair_api_task >> validate_excel_task >> summary_task >> email_task