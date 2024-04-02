# Projeto de Análise de Dados do YouTube

## Visão geral

Este projeto tem como objetivo gerenciar, agilizar e realizar análises com segurança em dados estruturados e semi-estruturados de vídeos do YouTube, com base nas categorias dos vídeos e nas métricas de tendências.

## Objetivos do Projeto

- **Ingestão de Dados:** Construir um mecanismo para coletar dados de diferentes fontes.
- **Sistema ETL:** Transformar os dados brutos em um formato adequado para análise.
- **Data Lake:** Armazenar os dados de várias fontes em um repositório centralizado.
- **Escalabilidade:** Garantir que o sistema seja capaz de lidar com o aumento do volume de dados.
- **Nuvem:** Utilizar a nuvem, neste caso a AWS, para processar grandes quantidades de dados, já que isso não seria possível em um computador local.
- **Relatórios:** Criar um painel para obter respostas às perguntas levantadas anteriormente.

## Serviços utilizados
- **Amazon S3:** Serviço de armazenamento de objetos que oferece escalabilidade, disponibilidade, segurança e performance.
- **AWS IAM:** Gerenciamento de identidade e acesso, permitindo controlar o acesso aos serviços e recursos da AWS com segurança.
- **Power BI:** Serviço de inteligência de negócios (BI)
- **AWS Glue:** Serviço de integração de dados serverless que facilita a descoberta, preparação e combinação de dados para análise, machine learning e desenvolvimento de aplicativos.
- **AWS Lambda:** Serviço de computação que permite aos programadores executar código sem criar ou gerenciar servidores.
- **AWS Athena:** Serviço de consulta interativa para S3, onde os dados permanecem no S3 sem necessidade de carregamento.

## Conjunto de Dados Utilizado
Este conjunto de dados do [Kaggle](https://www.kaggle.com/datasets/datasnaek/youtube-new) contém estatísticas (arquivos CSV) sobre vídeos populares do YouTube ao longo de vários meses. Existem até 200 vídeos em alta publicados todos os dias para muitos locais. Os dados para cada região estão em seu próprio arquivo.

O conjunto de dados inclui itens como título do vídeo, canal, data de publicação, tags, visualizações, curtidas e descurtidas, descrição e contagem de comentários. Um campo category_id, que difere por região, também está incluído no arquivo JSON vinculado à região.

## Arquitetura

![Arquitetura](/Projeto%20III/evidencias/Arquitetura.png)

## Passos realizados

Fazer o upload dos arquivos para o bucket S3 - Dados Brutos
    
- Acesse os comandos utilizados para enviar os arquivos para o S3 [Aqui](/Projeto%20III/evidencias/comandos-CLI.sh)

![Bucket](/Projeto%20III/evidencias/bucket-raw-criado.png)
![Pastas](/Projeto%20III/evidencias/pastas.png)
![Pastas](/Projeto%20III/evidencias/pastas1_csv.png)
![Pastas](/Projeto%20III/evidencias/pastas2_json.png)

No Glue criamos um Crawler para mapear a pasta RAW com os arquivos 
    - Adicionar permissão para que o GLue possa acessar os buckets S3 no IAM   
    - Criar banco de dados para armazenar as tabelas com os dados brutos que serão mapeadas pelo crawler 

![Database](/Projeto%20III/evidencias/database-raw.png)
![Dados](/Projeto%20III/evidencias/athena-dados-limpos.png)

Para consultar os dados dessa tabela criada, usamos o AWS Athena
    - Para usar o Athena devemos criar um local de saída para as consultas (salvar as consultas em um bucket S3)

Criamos uma função Lambda para converter os arquivos Json em Parquet
    - Criar função no IAM que de permissão para o lambda acessar o S3
    - Criar outro bucket para armazenar os dados limpos (cleaned) 
    - Executar a função lambda, os dados são limpos e convertidos em parquet

![Lambda](/Projeto%20III/evidencias/lambda-function.png)
![Variaveis](/Projeto%20III/evidencias/variaveis.png)
![Função ok](/Projeto%20III/evidencias/função-ok.png)

