-- Camada Marts (models/marts): Tabela Fato de Corridas NYC TLC (PK MD5: chave | FKs MD5: id_dim_*)
{{ config(materialized='table') }}

SELECT
    chave,
    id_dim_zonas_embarque,
    id_dim_zonas_desembarque,
    id_dim_modalidades_pagamento,
    id_dim_tarifas,
    data_ref,
    dh_embarque,
    dh_desembarque,
    hora_embarque,
    turno_viagem,
    tipo_rota,
    qtd_passageiros,
    distancia_milhas,
    distancia_km,
    duracao_minutos,
    velocidade_media_kmh,
    valor_tarifa_base,
    valor_extras_total,
    valor_gorjeta,
    pct_gorjeta,
    valor_pedagio,
    valor_taxa_congestionamento,
    valor_taxa_aeroporto,
    valor_total_corrida
FROM {{ ref('int_corridas_tratadas') }}
