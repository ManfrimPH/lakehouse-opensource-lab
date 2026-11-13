-- Camada Staging (models/staging): Espelho 1:1 de raw_taxi_zones sem nenhum tratamento (apenas tipagem e rename)
{{ config(materialized='view') }}

SELECT
    CAST(location_id AS INTEGER)  AS id_zona,
    CAST(borough AS TEXT)         AS distrito,
    CAST(zone AS TEXT)            AS nome_zona,
    CAST(service_zone AS TEXT)    AS zona_servico
FROM {{ source('raw', 'raw_taxi_zones') }}
