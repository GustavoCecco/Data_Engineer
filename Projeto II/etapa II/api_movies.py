import json
import requests
import boto3
import os
from datetime import datetime

api_key = os.environ['TMDB_API_KEY']
s3_prefix = "Raw/TMDB/JSON/2023/11/02/"
max_file_size = 100

def person_upload_api(event, context):
    file_data = []

    response = requests.get("https://api.themoviedb.org/3/person/popular?api_key=" + api_key)
    data = response.json()

    s3 = boto3.client('s3')

    for movie in data['results']:
        file_data.append(json.dumps(movie))

    if file_data:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = f"{timestamp}.json"
        s3_key = f"{s3_prefix}{file_name}"

        try:
            s3.put_object(Body='\n'.join(file_data), Bucket="data-lake-exemplo", Key=s3_key)
            print(f"Arquivo gerado e enviado para {s3_key}")
        except Exception as e:
            print(f"Erro ao enviar arquivo para o S3: {str(e)}")

    return {
        'statusCode': 200,
        'body': 'Dados coletados com sucesso!'
    }
