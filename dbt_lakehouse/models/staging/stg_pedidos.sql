-- Camada Staging (models/staging): Espelho 1:1 da source RAW sem nenhum tratamento (apenas tipagem e rename)
{{ config(materialized='view') }}

SELECT
    CAST(id_pedido AS INTEGER)          AS id_pedido,
    CAST(id_cliente AS INTEGER)         AS id_cliente,
    CAST(data_venda AS TIMESTAMP)       AS data_venda,
    CAST(status_pedido AS TEXT)         AS status_pedido,
    CAST(forma_pagamento AS TEXT)       AS forma_pagamento,
    CAST(valor_bruto AS NUMERIC(14,2))  AS valor_bruto,
    CAST(desconto AS NUMERIC(14,2))     AS desconto,
    CAST(valor_liquido AS NUMERIC(14,2)) AS valor_liquido
FROM {{ source('raw', 'raw_pedidos') }}
