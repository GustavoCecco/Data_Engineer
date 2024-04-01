# The Movie Database (TMBD)

## Sobre o Projeto

O objetivo deste projeto é realizar uma análise abrangente da filmografia de Denzel Washington. Utilizando técnicas de processamento de dados e visualização, pretendo fornecer insights interessantes sobre a carreira cinematográfica deste ícone da indústria do entretenimento.

## Fonte de Dados

Os dados serão coletados das APIs do The Movie Database (TMDB) e dos arquivos CSV. A coleta será realizada de forma a abranger toda a carreira de Denzel Washington, desde seus primeiros trabalhos até suas produções mais recentes.

## Etapa I

Nesta etapa o script Python faz o upload de arquivos CSV locais para um bucket S3 na AWS. Ele verifica se as credenciais da AWS estão disponíveis, cria um cliente S3, define nomes de arquivos e caminhos no S3, realiza o upload dos arquivos e imprime uma mensagem de sucesso. O Dockerfile é usado para criar um contêiner que executa o script Python quando iniciado. 

[**Etapa I - Clique aqui para acessar os arquivos**](/Projeto%20II/etapa%20I)


## Etapa II

Os códigos coletam informações da API TMDb sobre atores, filmes e pessoas populares e as armazenam em arquivos JSON no serviço S3 da AWS. Eles usam a chave da API TMDb armazenada como variável de ambiente para autenticação nas solicitações à API. A função credits_upload_api coleta informações específicas dos créditos de um ator, enquanto a função person_upload_api coleta informações sobre pessoas populares na TMDb.

[**Etapa II - Clique aqui para acessar os arquivos**](/Projeto%20II/etapa%20II)


## Etapa III

Em resumo, os códigos da etapa III são uma parte do pipeline de ETL no AWS Glue. Eles lêem dados de um catálogo do AWS Glue, realiza transformações no esquema e, em seguida, grava os dados transformados no Amazon S3 em formato Parquet. Aqui também temos o modelo dimensional criado para ingestão dos dados.

[**Etapa III - Clique aqui para acessar os arquivos**](/Projeto%20II/etapa%20III)

### Modelo Dimensional

![**Modelo Dimensional**](/Projeto%20II/evidencias/modelo_dimensional.jpg)


## Etapa IV

Na etapa IV temos o dashboard onde procuramos responder algumas questões como:

- Qual é o total de filmes dirigidos/produzidos pelo ator?
- Qual é o total de filmes em que atuou?
- Quais são os principais gêneros destes filmes?
- Qual foi a relação entre custo de produção e bilheteria?
- Em relação as avaliações, como se apresentam estes filmes?


[**Etapa IV - Clique aqui para acessar o dashboard**](/Projeto%20II/evidencias/Denzel%20Washington.pdf)