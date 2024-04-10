-- Criar um novo banco de dados
CREATE DATABASE DE_PROJECT;

-- Mudar para o banco de dados recém-criado
USE DATABASE DE_PROJECT;

-- Criar tabela para carregar dados CSV
CREATE or replace TABLE weather_data(
    temp       NUMBER(20,0),
    CITY          VARCHAR(128) 
    ,humidity   NUMBER(20,5)
    ,wind_speed      NUMBER(20,5) 
   ,time             VARCHAR(128)  
   ,wind_dir        VARCHAR(128)
   ,pressure_mb    NUMBER(20,5)
);

-- Criar objeto de integração para o stage externo
create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::185049995804:role/AWSSnowFLake'
  storage_allowed_locations = ('s3://project-weather-api/snowflake/');

-- Descrever o objeto de integração para obter o external_id e ser usado no s3
DESC INTEGRATION s3_int;

-- Criar formato de arquivo para CSV
create or replace file format csv_format
                    type = csv
                    field_delimiter = ','
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;

-- Criar stage para armazenamento externo
create or replace stage ext_csv_stage
  URL = 's3://project-weather-api/snowflake/'
  STORAGE_INTEGRATION = s3_int
  file_format = csv_format;

-- Criar pipe para automatizar a ingestão de dados do s3 para o Snowflake
create or replace pipe mypipe auto_ingest=true as
copy into weather_data
from @ext_csv_stage
on_error = CONTINUE;

-- Mostrar pipes criados
show pipes;

-- Selecionar todos os dados da tabela weather_data
select * from weather_data;
