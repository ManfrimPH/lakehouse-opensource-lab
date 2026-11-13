-- Camada Intermediate (models/intermediate): Tratamento de strings, nulos, cálculo de LTV/Ticket Médio e geração de MD5
{{ config(materialized='view') }}

WITH clientes_tratados AS (
    SELECT
        id_cliente,
        MD5(CAST(id_cliente AS TEXT))           AS id_dim_clientes,
        TRIM(nome_cliente)                      AS nome_cliente,
        UPPER(TRIM(segmento))                   AS segmento,
        UPPER(TRIM(uf))                         AS uf,
        TRIM(cidade)                            AS cidade,
        data_cadastro
    FROM {{ ref('stg_clientes') }}
),

pedidos_concluidos AS (
    SELECT
        id_pedido,
        id_cliente,
        data_venda,
        UPPER(TRIM(status_pedido))              AS status_pedido,
        COALESCE(valor_liquido, 0.00)           AS valor_liquido
    FROM {{ ref('stg_pedidos') }}
    WHERE UPPER(TRIM(status_pedido)) = 'CONCLUIDO'
),

metricas AS (
    SELECT
        id_cliente,
        COUNT(id_pedido)                        AS total_pedidos,
        COALESCE(SUM(valor_liquido), 0.00)      AS ltv_cliente,
        COALESCE(AVG(valor_liquido), 0.00)      AS ticket_medio,
        MIN(data_venda)                         AS primeira_compra,
        MAX(data_venda)                         AS ultima_compra
    FROM pedidos_concluidos
    GROUP BY id_cliente
)

SELECT
    c.id_dim_clientes,
    c.id_cliente,
    c.nome_cliente,
    c.segmento,
    c.uf,
    c.cidade,
    c.data_cadastro,
    COALESCE(m.total_pedidos, 0)                AS total_pedidos,
    COALESCE(m.ltv_cliente, 0.00)               AS ltv_cliente,
    COALESCE(m.ticket_medio, 0.00)              AS ticket_medio,
    CASE
        WHEN COALESCE(m.ltv_cliente, 0) >= 30000 THEN 'TIER_A_VIP'
        WHEN COALESCE(m.ltv_cliente, 0) >= 10000 THEN 'TIER_B_GROWTH'
        ELSE 'TIER_C_STANDARD'
    END                                         AS classificacao_cliente,
    m.primeira_compra,
    m.ultima_compra
FROM clientes_tratados c
LEFT JOIN metricas m
    ON c.id_cliente = m.id_cliente
