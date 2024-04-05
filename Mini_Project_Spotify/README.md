# Mini Projeto de Engenharia de Dados - Spotify

## Objetivo

O Objetivo desse projeto é criar um pipeline de dados com base em 3 arquivos CSV que podem ser acessados [Aqui](/Mini_Project_Spotify/data/). É um mini projeto para por em prática alguns conhecimentos que estou estudando e ter um contato com a Cloud AWS e alguns de seus serviços.

## Arquitetura

![Arquitetura](/Mini_Project_Spotify/arquitetura.png)

## Execução do projeto

**1 - Enviar os arquivos para o S3 e criar as pastas**
- Os arquivos foram enviados através do terminal do VS Code utilizando AWS CLI

![Enviando arquivos](/Mini_Project_Spotify/evidencias/enviando_csv.png)

![Arquivos salvos S3](/Mini_Project_Spotify/evidencias/csv_s3.png)

**2 - Criar Visual ETL com o Glue para tratar e enviar os dados para a pasta cleaned do s3**
- Criei um ETL através do visual ETL para ter um contato inicial com o Glue e gerar o script pronto para estudar.

![Pipeline](/Mini_Project_Spotify/evidencias/visual_etl.png)

**Acesse o script ETL [Aqui](/Mini_Project_Spotify/etl.py)**

**3 - Rodar o Glue Crawler para mapear os arquivos gerados anteriormente e criar as tabelas no athena**
- Criei um Crawler para ler os arquivos que foram gerados através do Visual ETL.
- Criei uma tabela no Athena para criar as tabelas através do Crawler
- Query no athena para testar se está funcionando

![Crawler](/Mini_Project_Spotify/evidencias/run-crawler.png)

![Tabela Athena](/Mini_Project_Spotify/evidencias/create-database.png)

![Query](/Mini_Project_Spotify/evidencias/query-athena.png)


**4 - Conectar com o Power BI**
- Aqui eu conectei o Athena com o Power BI através do ODBC conector para testar a funcionalidade, não criei nenhum visual.

![BI](/Mini_Project_Spotify/evidencias/odbc_BI.png)