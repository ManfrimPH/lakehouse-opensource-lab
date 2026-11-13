-- Camada Marts (models/marts): Dimensão Tarifas Regulatórias NYC TLC (PK MD5: id_dim_tarifas)
{{ config(materialized='table') }}

WITH dominio_tarifas AS (
    SELECT 1  AS cod_tarifa, 'TARIFA_PADRAO_URBANA'        AS nome_tarifa, 'TAXIMETRO_VARIAVEL'   AS tipo_cobranca UNION ALL
    SELECT 2  AS cod_tarifa, 'TARIFA_FIXA_AEROPORTO_JFK'   AS nome_tarifa, 'TARIFA_FIXA_TABELADA' AS tipo_cobranca UNION ALL
    SELECT 3  AS cod_tarifa, 'TARIFA_AEROPORTO_NEWARK'     AS nome_tarifa, 'TAXIMETRO_INTER_ESTADUAL' AS tipo_cobranca UNION ALL
    SELECT 4  AS cod_tarifa, 'SUBURBIO_NASSAU_WESTCHESTER' AS nome_tarifa, 'TAXIMETRO_VARIAVEL'   AS tipo_cobranca UNION ALL
    SELECT 5  AS cod_tarifa, 'TARIFA_ACORDADA_NEGOCIADA'   AS nome_tarifa, 'TARIFA_FIXA_TABELADA' AS tipo_cobranca UNION ALL
    SELECT 6  AS cod_tarifa, 'CORRIDA_COMPARTILHADA_GRUPO' AS nome_tarifa, 'TARIFA_FIXA_TABELADA' AS tipo_cobranca UNION ALL
    SELECT 99 AS cod_tarifa, 'TARIFA_NAO_INFORMADA'        AS nome_tarifa, 'TAXIMETRO_VARIAVEL'   AS tipo_cobranca
)

SELECT
    MD5(CAST(cod_tarifa AS TEXT)) AS id_dim_tarifas,
    nome_tarifa,
    tipo_cobranca
FROM dominio_tarifas
