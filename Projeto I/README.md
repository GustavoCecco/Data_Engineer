# ETL em ambiente containerizado

## Desafio

O objetivo deste projeto é a implementação de um pequeno Data Warehouse. Para isso, iremos desenvolver através da linguagem python um processo de ETL que extrai os dados existentes em arquivos no formato CSV e no banco de dados MongoDB e os persiste em banco de dados Postgres seguindo modelagem dimensional. 

**Acesse os códigos fonte aqui:**

- [docker-compose](/Projeto%20I/docker-compose.yaml)
- [Dockerfile](/Projeto%20I/python_etl/Dockerfile)
- [etl.py](/Projeto%20I/python_etl/etl.py)


## Modelo Dimensional
![Modelo Dimensional](/Projeto%20I/evidencias/modelo_dimensional.jpg)


## Funcionalidades do Script

Este script Python realiza as seguintes tarefas:

### Conexão ao PostgreSQL
O código começa definindo os parâmetros de conexão com o PostgreSQL, como host, porta, usuário, senha e nome do banco de dados. Em seguida, há uma função chamada postgresql_connection que tenta estabelecer uma conexão com o PostgreSQL com um limite máximo de tentativas. Se a conexão for bem-sucedida, retorna a conexão.

### Criação de Tabelas
Após a conexão ser estabelecida, o código cria várias tabelas no PostgreSQL. Essas tabelas são usadas para armazenar dados relacionados a clientes, produtos, pedidos, pagamentos, itens de pedido, avaliações e vendas. As tabelas são criadas com as devidas colunas e chaves primárias/estrangeiras.

### Leitura e Inserção de Dados
O código continua lendo dados de arquivos CSV para várias tabelas, como clientes, produtos, pedidos, pagamentos e itens de pedido. Os dados são lidos usando a biblioteca Pandas e inseridos nas tabelas correspondentes no PostgreSQL usando instruções SQL INSERT. Aqui também é feita a mesclagem dos CSV utilizando Pandas, dando origem ao CSV sales_df, através dele é possivel inserir os dados na tabela ft_sales.

### Recuperação de Dados do MongoDB
O código conecta-se ao MongoDB usando parâmetros de host e porta e recupera os dados da coleção order_reviews.

### Transformação e Inserção de Dados
Os dados recuperados do MongoDB são transformados e inseridos na tabela dim_reviews do PostgreSQL. Durante esse processo, há também uma validação do campo review_score para garantir que ele seja um número inteiro válido.

### Encerramento e Comprometimento
Finalmente, o código fecha a conexão com o MongoDB, compromete as transações no PostgreSQL e fecha o cursor.

## Erros persistentes
![Erro ao conectar ao PostgreSQL](/Projeto%20I/evidencias/erro_tcp.jpg)

### Solução
Criar a função postgresql_connection com o objetivo de tentar estabelecer a conexão várias vezes com o banco de dados PostgreSQL antes de finalizar, pois não estava conseguindo conectar.

![Erro ao carregar dados](/Projeto%20I/evidencias/erro_carregar_dados.jpg)

### Solução
Verificar e formatar o campo review_score para garantir que ele seja um número inteiro válido ou, em caso de problemas, fornecer informações relevantes sobre o tipo de dado encontrado ou a ausência do campo.

![Solução](/Projeto%20I/evidencias/solução.jpg)


# Views para consultas no banco
Criei algumas views que podem auxiliar a extrair algumas informações sobre os dados que estão no data warehouse.

[Acesse as views aqui](/Projeto%20I/evidencias/views.sql)