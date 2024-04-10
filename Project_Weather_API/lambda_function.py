import json
from datetime import datetime
import requests  
import boto3
from decimal import Decimal

# Inicializa o recurso DynamoDB e obtém a tabela "weather"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("weather")

# Faz a solicitação GET à API do tempo. Converte a resposta da API em um formato JSON.
def get_weather_data(city):  
    """
    Função para obter os dados meteorológicos de uma cidade da API do tempo.

    Parâmetros:
    - city: str, nome da cidade a ser consultada.

    Retorna:
    - data: dict, dados meteorológicos da cidade.
    """
    api_url = "http://api.weatherapi.com/v1/current.json"
    params = {  
        "q": city,    
        "key": "17d01fb1c8da4f05b2903510240904"
    }  
    response = requests.get(api_url, params=params)  
    data = response.json()  
    return data  

def lambda_handler(event, context):
    """
    Função principal do AWS Lambda para processar eventos.

    Parâmetros:
    - event: dict, evento recebido pelo Lambda.
    - context: objeto, contexto de execução do Lambda.

    Retorna:
    - None
    """
    # Lista de cidades a serem consultadas
    cities = ["Passo Fundo","Porto Alegre","Carazinho","Curitiba","São Paulo","Campinas","Uruguaiana","Marau","Chapeco","Marialva"]
    for city in cities:
        # Obtém os dados meteorológicos da cidade
        data = get_weather_data(city)  
    
        # Extrai as informações necessárias da resposta da API: temperatura, velocidade do vento, direção do vento, pressão atmosférica e umidade.
        temp = data['current']['temp_c']
        wind_speed = data['current']['wind_mph']
        wind_dir = data['current']['wind_dir']
        pressure_mb = data['current']['pressure_mb']
        humidity = data['current']['humidity']
    
        # Imprime os dados meteorológicos da cidade
        print(city,temp,wind_speed,wind_dir,pressure_mb,humidity)
        
        # Obtém o timestamp atual
        current_timestamp = datetime.utcnow().isoformat()
        
        # Cria um item a ser inserido no DynamoDB
        item = {
            'city': city,
            'time': str(current_timestamp),
            'temp': temp,
            'wind_speed': wind_speed,
            'wind_dir': wind_dir,
            'pressure_mb': pressure_mb,
            'humidity': humidity
        }
        
        # Converte os valores para Decimal
        item = json.loads(json.dumps(item), parse_float=Decimal)
        
        # Insere os dados no DynamoDB
        table.put_item(Item=item)
