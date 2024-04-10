from datetime import datetime
import pandas as pd
import boto3
from io import StringIO

def handle_insert(record):
    """
    Função para lidar com a inserção de dados no DynamoDB.
    
    Parâmetros:
    - record: dict, registro do DynamoDB
    
    Retorna:
    - dff: DataFrame, DataFrame contendo os dados inseridos
    """
    print("Handling Insert: ", record)
    dict = {}

    # Itera sobre os itens do NewImage do registro
    for key, value in record['dynamodb']['NewImage'].items():
        for dt, col in value.items():
            dict.update({key: col})

    # Cria um DataFrame a partir do dicionário
    dff = pd.DataFrame([dict])
    return dff

def lambda_handler(event, context):
    """
    Função principal do AWS Lambda para processar eventos.
    
    Parâmetros:
    - event: dict, evento recebido pelo Lambda
    - context: objeto, contexto de execução do Lambda
    
    Retorna:
    - None
    """
    print(event)
    df = pd.DataFrame()

    # Itera sobre os registros no evento
    for record in event['Records']:
        # Extrai o nome da tabela do ARN do evento
        table = record['eventSourceARN'].split("/")[1]

        # Verifica se o evento é uma inserção
        if record['eventName'] == "INSERT": 
            # Chama a função para lidar com a inserção
            dff = handle_insert(record)
        df = dff

    # Verifica se o DataFrame não está vazio
    if not df.empty:
        # Converte todas as colunas para string
        all_columns = list(df)
        df[all_columns] = df[all_columns].astype(str)

        # Cria o nome do arquivo no S3
        path = table + "_" + str(datetime.now()) + ".csv"
        print(event)

        # Cria um buffer de CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)

        # Conecta ao serviço S3
        s3 = boto3.client('s3')
        bucketName = "project-weather-api"
        key = "snowflake/" + table + "_" + str(datetime.now()) + ".csv"
        print(key)
        
        # Envia o arquivo CSV para o S3
        s3.put_object(Bucket=bucketName, Key=key, Body=csv_buffer.getvalue(),)

    print('Successfully processed %s records.' % str(len(event['Records'])))
