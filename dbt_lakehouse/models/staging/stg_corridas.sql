-- Camada Staging (models/staging): Espelho 1:1 de raw_yellow_tripdata sem nenhum tratamento (apenas tipagem e rename)
{{ config(materialized='view') }}

SELECT
    CAST(trip_id AS BIGINT)                      AS id_corrida_origem,
    CAST(vendorid AS INTEGER)                    AS id_fornecedor,
    CAST(ratecodeid AS INTEGER)                  AS id_tarifa,
    CAST(payment_type AS INTEGER)                AS id_tipo_pagamento,
    CAST(pulocationid AS INTEGER)                AS id_zona_embarque,
    CAST(dolocationid AS INTEGER)                AS id_zona_desembarque,
    CAST(tpep_pickup_datetime AS TIMESTAMP)      AS dh_embarque,
    CAST(tpep_dropoff_datetime AS TIMESTAMP)     AS dh_desembarque,
    CAST(passenger_count AS INTEGER)             AS qtd_passageiros,
    CAST(trip_distance AS NUMERIC(12,2))         AS distancia_milhas,
    CAST(store_and_fwd_flag AS TEXT)             AS flag_armazenou_enviou,
    CAST(fare_amount AS NUMERIC(12,2))           AS valor_tarifa_base,
    CAST(extra AS NUMERIC(12,2))                 AS valor_extra,
    CAST(mta_tax AS NUMERIC(12,2))               AS valor_imposto_mta,
    CAST(tip_amount AS NUMERIC(12,2))            AS valor_gorjeta,
    CAST(tolls_amount AS NUMERIC(12,2))          AS valor_pedagio,
    CAST(improvement_surcharge AS NUMERIC(12,2)) AS valor_sobretaxa_melhoria,
    CAST(congestion_surcharge AS NUMERIC(12,2))  AS valor_taxa_congestionamento,
    CAST(airport_fee AS NUMERIC(12,2))           AS valor_taxa_aeroporto,
    CAST(total_amount AS NUMERIC(12,2))          AS valor_total_corrida
FROM {{ source('raw', 'raw_yellow_tripdata') }}
