import boto3
import os

aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')

if aws_access_key_id and aws_secret_access_key:
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')

    movies_csv_file = 'movies.csv'
    series_csv_file = 'series.csv'

    movies_s3_path = 'Raw/Local/CSV/Movies/2023/10/29/'
    series_s3_path = 'Raw/Local/CSV/Series/2023/10/29/'

    bucket_name = 'data-lake-exemplo'

    s3.upload_file(movies_csv_file, bucket_name, os.path.join(movies_s3_path, movies_csv_file))
    s3.upload_file(series_csv_file, bucket_name, os.path.join(series_s3_path, series_csv_file))

    print("Arquivos enviados com sucesso para o bucket S3!")
else:
    print("Credenciais da AWS não foram encontradas. Verifique as variáveis de ambiente.")
