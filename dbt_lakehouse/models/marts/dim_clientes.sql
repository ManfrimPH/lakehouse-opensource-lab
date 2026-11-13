-- Camada Marts (models/marts): Dimensão Corporativa de Clientes (PK MD5: id_dim_clientes)
{{ config(materialized='table') }}

SELECT
    id_dim_clientes,
    nome_cliente,
    segmento,
    uf,
    cidade,
    data_cadastro,
    total_pedidos,
    ltv_cliente,
    ticket_medio,
    classificacao_cliente,
    primeira_compra,
    ultima_compra
FROM {{ ref('int_clientes_metricas') }}
