-- Camada Marts (models/marts): Mart Executivo de Faturamento Diário (PK MD5: chave | FK MD5: id_dim_clientes)
{{ config(materialized='table') }}

SELECT
    MD5(
        CAST(data_ref AS TEXT) || '|' ||
        id_dim_clientes || '|' ||
        COALESCE(forma_pagamento, '')
    )                               AS chave,
    id_dim_clientes,
    data_ref,
    nome_cliente,
    segmento,
    uf,
    forma_pagamento,
    COUNT(chave)                    AS qtd_pedidos,
    SUM(valor_bruto)                AS total_bruto,
    SUM(desconto)                   AS total_descontos,
    SUM(valor_liquido)              AS total_vendido,
    SUM(valor_taxa_gateway)         AS total_taxas_gateway,
    SUM(receita_liquida_real)       AS receita_liquida_pos_taxa
FROM {{ ref('fato_vendas') }}
WHERE status_pedido = 'CONCLUIDO'
GROUP BY
    data_ref,
    id_dim_clientes,
    nome_cliente,
    segmento,
    uf,
    forma_pagamento
