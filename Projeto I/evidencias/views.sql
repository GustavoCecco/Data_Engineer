-- Métricas gerais de vendas
CREATE VIEW vw_vendas_gerais AS
SELECT
    f.order_id,
    f.customer_id,
    f.product_id,
    f.payment_installments,
    f.payment_value,
    f.price,
    f.freight_value,
    r.review_score
FROM
    ft_sales f
JOIN
    dim_reviews r
ON
    f.order_id = r.order_id;


-- Média de Escore de Avaliação por Produto
CREATE VIEW vw_avaliacao_por_produto AS
SELECT
    p.product_id,
    AVG(m.review_score) AS average_review_score
FROM
    vw_sales_metrics m
JOIN
    dim_product p
ON
    m.product_id = p.product_id
GROUP BY
    p.product_id;

-- Total de Vendas por Mês
CREATE VIEW vw_vendas_por_mes AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp) AS purchase_month,
    SUM(f.payment_value) AS total_sales
FROM
    ft_sales f
JOIN
    dim_orders o
ON
    f.order_id = o.order_id
GROUP BY
    purchase_month
ORDER BY
    purchase_month;

-- Número de Parcelas Médio por Categoria de Produto
CREATE VIEW vw_parcelas_por_categoria AS
SELECT
    p.product_category_name,
    AVG(f.payment_installments) AS avg_installments
FROM
    ft_sales f
JOIN
    dim_product p
ON
    f.product_id = p.product_id
GROUP BY
    p.product_category_name;


-- Custo de Frete Médio por Estado do Cliente
CREATE VIEW vw_frete_medio_por_estado AS
SELECT
    c.customer_state,
    AVG(f.freight_value) AS avg_freight_cost
FROM
    ft_sales f
JOIN
    dim_customer c
ON
    f.customer_id = c.customer_id
GROUP BY
    c.customer_state;
