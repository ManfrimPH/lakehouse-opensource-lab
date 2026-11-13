-- Camada Staging (models/staging): Espelho 1:1 da source RAW sem nenhum tratamento (apenas tipagem e rename)
{{ config(materialized='view') }}

SELECT
    CAST(id_cliente AS INTEGER)   AS id_cliente,
    CAST(nome_cliente AS TEXT)    AS nome_cliente,
    CAST(segmento AS TEXT)        AS segmento,
    CAST(uf AS TEXT)              AS uf,
    CAST(cidade AS TEXT)          AS cidade,
    CAST(data_cadastro AS DATE)   AS data_cadastro
FROM {{ source('raw', 'raw_clientes') }}
