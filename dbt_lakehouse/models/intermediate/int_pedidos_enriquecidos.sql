-- Camada Intermediate (models/intermediate): Tratamento de strings, nulos, joins e geração de chaves MD5 de pedidos
{{ config(materialized='view') }}

WITH pedidos_tratados AS (
    SELECT
        id_pedido,
        id_cliente,
        data_venda,
        CAST(DATE_TRUNC('day', data_venda) AS DATE) AS data_ref,
        UPPER(TRIM(status_pedido))                  AS status_pedido,
        UPPER(TRIM(forma_pagamento))                AS forma_pagamento,
        COALESCE(valor_bruto, 0.00)                 AS valor_bruto,
        COALESCE(desconto, 0.00)                    AS desconto,
        COALESCE(valor_liquido, 0.00)               AS valor_liquido
    FROM {{ ref('stg_pedidos') }}
),

pagamentos_tratados AS (
    SELECT
        id_pagamento,
        id_pedido,
        UPPER(TRIM(meio_pagamento))                 AS meio_pagamento,
        COALESCE(parcelas, 1)                       AS parcelas,
        COALESCE(taxa_gateway_pct, 0.00)            AS taxa_gateway_pct,
        COALESCE(valor_taxa_gateway, 0.00)          AS valor_taxa_gateway,
        UPPER(TRIM(status_liquidacao))              AS status_liquidacao
    FROM {{ ref('stg_pagamentos') }}
),

clientes_tratados AS (
    SELECT
        id_cliente,
        TRIM(nome_cliente)                          AS nome_cliente,
        UPPER(TRIM(segmento))                       AS segmento,
        UPPER(TRIM(uf))                             AS uf,
        TRIM(cidade)                                AS cidade
    FROM {{ ref('stg_clientes') }}
)

SELECT
    MD5(CAST(p.id_pedido AS TEXT))                  AS chave,
    MD5(CAST(p.id_cliente AS TEXT))                 AS id_dim_clientes,
    p.id_pedido,
    p.id_cliente,
    pg.id_pagamento,
    c.nome_cliente,
    c.segmento,
    c.uf,
    c.cidade,
    p.data_venda,
    p.data_ref,
    p.status_pedido,
    p.forma_pagamento,
    COALESCE(pg.parcelas, 1)                        AS parcelas,
    p.valor_bruto,
    p.desconto,
    p.valor_liquido,
    COALESCE(pg.taxa_gateway_pct, 0.00)             AS taxa_gateway_pct,
    COALESCE(pg.valor_taxa_gateway, 0.00)           AS valor_taxa_gateway,
    (p.valor_liquido - COALESCE(pg.valor_taxa_gateway, 0.00)) AS receita_liquida_real,
    COALESCE(pg.status_liquidacao, 'PENDENTE')      AS status_liquidacao
FROM pedidos_tratados p
INNER JOIN clientes_tratados c
    ON p.id_cliente = c.id_cliente
LEFT JOIN pagamentos_tratados pg
    ON p.id_pedido = pg.id_pedido
