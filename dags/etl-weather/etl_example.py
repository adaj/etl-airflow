"""
ETL Example Script - Daily Weather Report

This script demonstrates a simple ETL process that:
1. Extracts weather data from OpenWeatherMap API
2. Transforms the data into a readable format
3. Loads the data by sending it via email

Usage:
    python etl_example.py --city "São Paulo" --email "user@example.com" --api_key "your_api_key"
"""

import requests
import json
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import fire
import os

def extract_weather_data(city, api_key):
    """Extract current weather data from OpenWeatherMap API"""
    print(f"🌤️  Extraindo dados do clima para {city}...")
    
    base_url = "https://api.openweathermap.org/data/2.5"
    current_url = f"{base_url}/weather"
    
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',
        'lang': 'pt_br'
    }
    
    try:
        response = requests.get(current_url, params=params, timeout=10)
        response.raise_for_status()
        weather_data = response.json()
        
        print(f"✅ Dados extraídos com sucesso!")
        return weather_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao extrair dados: {e}")
        raise

def transform_weather_data(weather_data):
    """Transform raw weather data into readable format"""
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
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print("✅ Dados transformados com sucesso!")
        return transformed
        
    except KeyError as e:
        print(f"❌ Erro ao transformar dados: Campo {e} não encontrado")
        raise

def generate_html_report(data):
    """Generate HTML report from transformed data"""
    print("📝 Gerando relatório HTML...")
    
    # Weather emoji mapping
    weather_emojis = {
        'clear': '☀️',
        'clouds': '☁️',
        'rain': '🌧️',
        'snow': '❄️',
        'thunderstorm': '⛈️',
        'mist': '🌫️',
        'fog': '🌫️'
    }
    
    # Get appropriate emoji
    description_lower = data['description'].lower()
    emoji = '🌤️'  # default
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
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>{emoji} Relatório do Clima</h1>
                <h2>{data['city']}, {data['country']}</h2>
                <p>Atualizado em: {data['timestamp']}</p>
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

def send_email(html_content, recipient_email, city):
    """Send email with weather report"""
    print(f"📧 Enviando email para {recipient_email}...")
    
    subject = f"🌤️ Relatório do Clima - {city} - {datetime.now().strftime('%d/%m/%Y')}"
    
    # Email configuration (using Gmail SMTP)
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = os.getenv('SENDER_EMAIL', 'sender@example.com')
    sender_password = os.getenv('SENDER_PASSWORD')  # App password
    
    if not sender_password:
        print("❌ SENDER_PASSWORD environment variable not set!")
        print("Please set your Gmail app password: export SENDER_PASSWORD='your_app_password'")
        return
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        
        msg.attach(MIMEText(html_content, 'html'))
        
        # Connect and send
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        print("✅ Email enviado com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao enviar email: {e}")
        raise

def run_etl(city, email, api_key):
    """Run the complete ETL process"""
    print("🚀 Iniciando processo ETL...")
    print("=" * 50)
    
    try:
        # Extract
        weather_data = extract_weather_data(city, api_key)
        
        # Transform
        transformed_data = transform_weather_data(weather_data)
        
        # Load (Generate and send report)
        html_report = generate_html_report(transformed_data)
        send_email(html_report, email, city)
        
        print("=" * 50)
        print("🎉 Processo ETL concluído com sucesso!")
        
    except Exception as e:
        print("=" * 50)
        print(f"❌ Erro no processo ETL: {e}")
        raise

def main(city="São Paulo", email="recipient@example.com", api_key=None):
    """Main function to run the ETL process"""
    api_key = api_key or os.getenv('OPENWEATHER_API_KEY')
    
    if not api_key:
        raise ValueError("API key is required. Set OPENWEATHER_API_KEY environment variable or pass --api_key")
    
    run_etl(city, email, api_key)

if __name__ == "__main__":
    fire.Fire(main)