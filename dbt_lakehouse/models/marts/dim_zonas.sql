-- Camada Marts (models/marts): Dimensão Zonas de Mobilidade Urbana NYC TLC (PK MD5: id_dim_zonas)
{{ config(materialized='table') }}

SELECT
    id_dim_zonas,
    distrito,
    nome_zona,
    zona_servico,
    total_embarques,
    total_desembarques,
    faturamento_origem_usd,
    tier_demanda_zona
FROM {{ ref('int_zonas_tratadas') }}
