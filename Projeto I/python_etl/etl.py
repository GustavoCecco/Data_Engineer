import time
import pymongo
import psycopg2
import pandas as pd

pg_host = "postgres" 
pg_port = 5432
pg_user = "gustavo"
pg_password = "gustavocecco"
pg_db = "pb_dw"

mongo_host = "mongodb"
mongo_port = 27017
mongo_db = "ecommerce"
mongo_collection = "order_reviews"


def connect_to_postgresql():
    max_attempts = 10
    attempt = 0

    while attempt < max_attempts:
        try:
            connection = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                user=pg_user,
                password=pg_password,
                database=pg_db
            )
            print("Conexão com o PostgreSQL estabelecida com sucesso.")
            return connection
        except Exception as e:
            print(f"Tentativa {attempt + 1}: Erro ao conectar ao PostgreSQL: {e}")
            attempt += 1
            time.sleep(5)  

    print(f"Atenção: Não foi possível conectar ao PostgreSQL após {max_attempts} tentativas.")
    return None


if __name__ == "__main__":
    postgres_connection = connect_to_postgresql()

# Criando e inserindo os dados dos arquivos CSV em todas as tabelas no PostgreSQL
    if postgres_connection:
        try:
            cursor = postgres_connection.cursor()
            create_table_sql = """
                CREATE TABLE dim_customer (
                    customer_id VARCHAR(255) PRIMARY KEY,
                    customer_unique_id VARCHAR(255),
                    customer_zip_code_prefix INTEGER,
                    customer_city VARCHAR(255),
                    customer_state VARCHAR(255)
                );

                CREATE TABLE dim_product (
                    product_id VARCHAR(255) PRIMARY KEY,
                    product_category_name VARCHAR(255),
                    product_name_length NUMERIC,
                    product_description_lenght NUMERIC,
                    product_photos_qty NUMERIC,
                    product_weight_g NUMERIC,
                    product_length_cm NUMERIC,
                    product_height_cm NUMERIC,
                    product_width_cm NUMERIC
                );

                CREATE TABLE dim_orders (
                    order_id VARCHAR(255) PRIMARY KEY,
                    customer_id VARCHAR(255),
                    order_status VARCHAR(50),
                    order_purchase_timestamp TIMESTAMP,
                    order_approved_at TIMESTAMP,
                    order_delivered_carrier_date TIMESTAMP,
                    order_delivered_customer_date TIMESTAMP,
                    order_estimated_delivery_date TIMESTAMP
                );

                CREATE TABLE dim_payments (
                    order_id VARCHAR(255),
                    payment_sequential INTEGER,
                    payment_type VARCHAR(50),
                    payment_installments INTEGER,
                    payment_value NUMERIC(10, 2)
                );

                CREATE TABLE dim_order_items (
                    order_id VARCHAR(255),
                    order_item_id INTEGER,
                    product_id VARCHAR(255),
                    seller_id VARCHAR(255),
                    shipping_limit_date TIMESTAMP,
                    price NUMERIC(10, 2),
                    freight_value NUMERIC(10, 2)
                );

                CREATE TABLE IF NOT EXISTS dim_reviews (
                    review_entry_id SERIAL PRIMARY KEY,
                    review_id VARCHAR(255),
                    order_id VARCHAR(255),
                    review_score INTEGER,
                    review_comment_title TEXT,
                    review_comment_message TEXT,
                    review_creation_date TIMESTAMP,
                    review_answer_timestamp TIMESTAMP
                );

                CREATE TABLE ft_sales (
                    order_id VARCHAR(255),
                    customer_id VARCHAR(255),
                    product_id VARCHAR(255),
                    payment_installments INTEGER,
                    payment_value NUMERIC(10, 2),
                    price NUMERIC(10, 2),
                    freight_value NUMERIC(10, 2),
                    FOREIGN KEY (order_id) REFERENCES dim_orders (order_id),
                    FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
                    FOREIGN KEY (product_id) REFERENCES dim_product (product_id)
                );
           """
            
            cursor.execute(create_table_sql)
            postgres_connection.commit()
            print("Tabelas criadas com sucesso no PostgreSQL.")

            # Lendo os arquivos CSV para inseri-los nas tabelas do Postgres
            customers_df = pd.read_csv('/app/input/olist_customers_dataset.csv').dropna()
            products_df = pd.read_csv("/app/input/olist_products_dataset.csv").dropna()
            orders_df = pd.read_csv("/app/input/olist_orders_dataset.csv").dropna()
            payments_df = pd.read_csv("/app/input/olist_order_payments_dataset.csv").dropna()
            order_items_df = pd.read_csv("/app/input/olist_order_items_dataset.csv").dropna()
            
            sales_df = orders_df.merge(order_items_df, on='order_id', how='inner')\
                .merge(products_df, on='product_id', how='inner')\
                .merge(customers_df, on='customer_id', how='inner')\
                .merge(payments_df, on='order_id', how='inner')
            
            selected_df = sales_df[['order_id', 'customer_id', 'product_id', 'payment_installments', 'payment_value', 'price', 'freight_value']]

            # Salvar a tabela mesclada em um arquivo CSV
            selected_df.to_csv('sales.csv', index=False)
            

            for index, row in customers_df.iterrows():
                cursor.execute(
                    "INSERT INTO dim_customer (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES (%s, %s, %s, %s, %s)",
                    (row['customer_id'], row['customer_unique_id'], row['customer_zip_code_prefix'], row['customer_city'], row['customer_state'])
                )

            for index, row in products_df.iterrows():
                cursor.execute(
                    "INSERT INTO dim_product (product_id, product_category_name, product_name_length, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (row['product_id'], row['product_category_name'], row['product_name_lenght'], row['product_description_lenght'], row['product_photos_qty'], row['product_weight_g'], row['product_length_cm'], row['product_height_cm'], row['product_width_cm'])
                )

            for index, row in orders_df.iterrows():
                cursor.execute(
                    "INSERT INTO dim_orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (row['order_id'], row['customer_id'], row['order_status'], row['order_purchase_timestamp'], row['order_approved_at'], row['order_delivered_carrier_date'], row['order_delivered_customer_date'], row['order_estimated_delivery_date'])
                )

            for index, row in payments_df.iterrows():
                cursor.execute(
                    "INSERT INTO dim_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value) VALUES (%s, %s, %s, %s, %s)",
                    (row['order_id'], row['payment_sequential'], row['payment_type'], row['payment_installments'], row['payment_value'])
                )

            for index, row in order_items_df.iterrows():
                cursor.execute(
                    "INSERT INTO dim_order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (row['order_id'], row['order_item_id'], row['product_id'], row['seller_id'], row['shipping_limit_date'], row['price'], row['freight_value'])
                )

            for index, row in sales_df.iterrows():
                cursor.execute(
                    "INSERT INTO ft_sales (order_id, customer_id, product_id, payment_installments, payment_value, price, freight_value) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (row['order_id'], row['customer_id'], row['product_id'], row['payment_installments'], row['payment_value'], row['price'], row['freight_value'])
                )
            

            mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
            mongo_db = mongo_client[mongo_db]
            mongo_collection = mongo_db[mongo_collection]
            mongo_data = list(mongo_collection.find())
            
            postgres_cursor = postgres_connection.cursor()
            

            for document in mongo_data:
                if 'review_score' in document:
                    if isinstance(document['review_score'], (int, float, str)):
                        try:
                            review_score = int(document['review_score'])
                        except ValueError:
                            review_score = 'Erro na conversão'
                    else:
                        review_score = 'Tipo de dado inválido'
                else:
                    review_score = 'N/A'

                # Inserir dados na tabela dim_reviews
                cursor.execute(
                    "INSERT INTO dim_reviews (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        document['review_id'],
                        document['order_id'],
                        review_score,
                        document.get('review_comment_title', 'N/A'),
                        document.get('review_comment_message', 'N/A'),
                        pd.to_datetime(document['review_creation_date']),
                        pd.to_datetime(document['review_answer_timestamp'])
                    )
                )

                postgres_connection.commit()
                mongo_client.close()
                postgres_connection.commit()
            print("Dados carregados com sucesso nas tabelas.")

        except Exception as e:
            print(f"Erro ao carregar dados: {e}")
        finally:
            cursor.close()

        