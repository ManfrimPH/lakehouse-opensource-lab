-- Camada Marts (models/marts): Cubo Analítico Diário de Mobilidade por Zona e Turno (PK MD5: chave | FK MD5: id_dim_zonas)
{{ config(materialized='table') }}

SELECT
    MD5(
        CAST(data_ref AS TEXT) || '|' ||
        id_dim_zonas_embarque || '|' ||
        turno_viagem
    )                                                   AS chave,
    id_dim_zonas_embarque                               AS id_dim_zonas,
    data_ref,
    turno_viagem,
    distrito_embarque,
    nome_zona_embarque,
    COUNT(chave)                                        AS qtd_corridas,
    SUM(qtd_passageiros)                                AS total_passageiros,
    ROUND(SUM(distancia_km), 2)                         AS distancia_total_km,
    ROUND(AVG(distancia_km), 2)                         AS distancia_media_km,
    ROUND(AVG(duracao_minutos), 2)                      AS duracao_media_min,
    ROUND(AVG(velocidade_media_kmh), 2)                 AS velocidade_media_kmh,
    ROUND(SUM(valor_total_corrida), 2)                  AS faturamento_bruto_usd,
    ROUND(SUM(valor_gorjeta), 2)                        AS total_gorjetas_usd,
    ROUND(SUM(valor_taxa_congestionamento), 2)          AS total_taxa_congestionamento_usd,
    ROUND(AVG(valor_total_corrida), 2)                  AS ticket_medio_usd,
    ROUND(
        CASE
            WHEN SUM(distancia_km) > 0 THEN SUM(valor_total_corrida) / SUM(distancia_km)
            ELSE 0.00
        END,
        2
    )                                                   AS receita_media_por_km_usd
FROM {{ ref('int_corridas_tratadas') }}
GROUP BY
    data_ref,
    id_dim_zonas_embarque,
    turno_viagem,
    distrito_embarque,
    nome_zona_embarque
