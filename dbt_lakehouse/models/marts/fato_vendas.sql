-- Camada Marts (models/marts): Tabela Fato de Vendas (PK MD5: chave | FK MD5: id_dim_clientes)
{{ config(materialized='table') }}

SELECT
    chave,
    id_dim_clientes,
    nome_cliente,
    segmento,
    uf,
    cidade,
    data_venda,
    data_ref,
    status_pedido,
    forma_pagamento,
    parcelas,
    valor_bruto,
    desconto,
    valor_liquido,
    valor_taxa_gateway,
    receita_liquida_real,
    status_liquidacao
FROM {{ ref('int_pedidos_enriquecidos') }}
