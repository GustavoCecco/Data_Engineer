# Projeto - Weather API

## Arquitetura

![Arquitetura](/Project_Weather_API/evidencias/arquitetura.png)

## Objetivo

O objetivo do projeto é criar um sistema de computação em nuvem na AWS que extrai dados climáticos de uma API externa (Weather API), transforma os dados e os carrega em um data warehouse na Snowflake.

- **Extrair dados climáticos:** A API de clima é usada para extrair dados climáticos de uma fonte externa. 
- **Transformar dados climáticos:** O Lambda é usado para transformar os dados climáticos em um formato adequado para o Snowflake. A transformação pode envolver a limpeza dos dados, a conversão de formatos de dados e a adição de novas colunas ou valores.
- **Carregar dados climáticos no Snowflake:** O Snowflake é usado para carregar os dados climáticos do S3 para o data warehouse. O Snowflake oferece alto desempenho e escalabilidade para análises de dados complexas.

## Funcionamento do Sistema

- A API de clima é chamada para extrair dados climáticos.
- Os dados climáticos são armazenados no DynamoDB.
- O Lambda é usado para transformar os dados climáticos em um formato adequado para o Snowflake.
- Os dados climáticos transformados são carregados no S3.
- O Snowflake é usado para carregar os dados climáticos do S3 para o data warehouse.
- O STS é usado para fornecer credenciais de segurança temporárias para os componentes do sistema.


## Passo a Passo

### Criar a tabela "Weather" no DynamoDB

![Dynamo](/Project_Weather_API/evidencias/criando-tabela-weather-dynamo.png)

### Criar a função lambda para salvar os dados da API no Dynamo com layer para requests

**Acesse o código dessa função [Aqui](/Project_Weather_API/lambda_function.py)**

![Function 1](/Project_Weather_API/evidencias/function-weather-1.png)
![Layer](/Project_Weather_API/evidencias/layer-requests.png)

### Executar a função lambda

![Function 1](/Project_Weather_API/evidencias/function-ok.png)
![Dynamo-Items](/Project_Weather_API/evidencias/dynamo-items.png)

### Criar S3 bucket para salvar os dados do dynamo em CSV 

![Bucket](/Project_Weather_API/evidencias/bucket-s3.png)

### Função lambda para pegar os dados do Dynamo e salvar no bucket s3 em CSV com layer para pandas
A função converte os dados das tabelas do Dynamo em CSV para salvar no S3

**Acesse o código dessa função [Aqui](/Project_Weather_API/lambda_dynamo_to_s3.py)**

![Function 2](/Project_Weather_API/evidencias/function-weather-2.png)

### Criar Trigger de ativação Dynamo DB (Sempre que forem inseridos dados novos no Dynamo, automaticamente serão enviados para o bucket S3)

![Trigger](/Project_Weather_API/evidencias/add-trigger.png)
![Trigger 2](/Project_Weather_API/evidencias/add-trigger-ok.png)

### Após os dados salvos no bucket, criar o banco de dados no SnowFlake e suas tabelas

![Function 1](/Project_Weather_API/evidencias/query-sf.png)

**Acesse o código em SQL [Aqui](/Project_Weather_API/snowflake.sql)**