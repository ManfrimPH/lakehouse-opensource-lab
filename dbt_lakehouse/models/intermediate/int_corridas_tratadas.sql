-- Camada Intermediate (models/intermediate): TODO o tratamento de corridas NYC TLC (nulos, filtros, conversões km/velocidade, joins e MD5)
{{ config(materialized='view') }}

WITH corridas_saneadas AS (
    SELECT
        id_corrida_origem,
        COALESCE(id_fornecedor, 99) AS id_fornecedor,
        CASE
            WHEN id_tarifa IN (1, 2, 3, 4, 5, 6) THEN id_tarifa
            ELSE 99
        END AS id_tarifa,
        CASE
            WHEN id_tipo_pagamento IN (1, 2, 3, 4, 5, 6) THEN id_tipo_pagamento
            ELSE 5
        END AS id_tipo_pagamento,
        COALESCE(id_zona_embarque, 264) AS id_zona_embarque,
        COALESCE(id_zona_desembarque, 264) AS id_zona_desembarque,
        dh_embarque,
        dh_desembarque,
        CASE
            WHEN COALESCE(qtd_passageiros, 0) <= 0 THEN 1
            ELSE qtd_passageiros
        END AS qtd_passageiros,
        COALESCE(distancia_milhas, 0.00) AS distancia_milhas,
        ROUND(COALESCE(distancia_milhas, 0.00) * 1.60934, 2) AS distancia_km,
        ROUND(CAST(EXTRACT(EPOCH FROM (dh_desembarque - dh_embarque)) / 60.0 AS NUMERIC), 2) AS duracao_minutos,
        COALESCE(valor_tarifa_base, 0.00) AS valor_tarifa_base,
        (COALESCE(valor_extra, 0.00) + COALESCE(valor_imposto_mta, 0.00) + COALESCE(valor_sobretaxa_melhoria, 0.00)) AS valor_extras_total,
        GREATEST(COALESCE(valor_gorjeta, 0.00), 0.00) AS valor_gorjeta,
        GREATEST(COALESCE(valor_pedagio, 0.00), 0.00) AS valor_pedagio,
        GREATEST(COALESCE(valor_taxa_congestionamento, 0.00), 0.00) AS valor_taxa_congestionamento,
        GREATEST(COALESCE(valor_taxa_aeroporto, 0.00), 0.00) AS valor_taxa_aeroporto,
        COALESCE(valor_total_corrida, 0.00) AS valor_total_corrida
    FROM {{ ref('stg_corridas') }}
    WHERE COALESCE(valor_total_corrida, 0) > 0
      AND COALESCE(valor_tarifa_base, 0) >= 0
      AND COALESCE(distancia_milhas, 0) >= 0
      AND dh_desembarque > dh_embarque
)

SELECT
    -- PK da Fato (chave) e FKs das Dimensões (id_dim_*) todas em MD5
    MD5(CAST(c.id_corrida_origem AS TEXT))      AS chave,
    MD5(CAST(c.id_zona_embarque AS TEXT))       AS id_dim_zonas_embarque,
    MD5(CAST(c.id_zona_desembarque AS TEXT))    AS id_dim_zonas_desembarque,
    MD5(CAST(c.id_tipo_pagamento AS TEXT))      AS id_dim_modalidades_pagamento,
    MD5(CAST(c.id_tarifa AS TEXT))              AS id_dim_tarifas,
    -- Chaves de origem usadas apenas até a camada Intermediate
    c.id_corrida_origem,
    c.id_zona_embarque,
    c.id_zona_desembarque,
    c.id_tipo_pagamento,
    c.id_tarifa,
    -- Atributos Temporais e Turno
    CAST(DATE_TRUNC('day', c.dh_embarque) AS DATE) AS data_ref,
    c.dh_embarque,
    c.dh_desembarque,
    CAST(EXTRACT(HOUR FROM c.dh_embarque) AS INTEGER) AS hora_embarque,
    CASE
        WHEN EXTRACT(HOUR FROM c.dh_embarque) BETWEEN 6 AND 9 THEN '01_PICO_MANHA'
        WHEN EXTRACT(HOUR FROM c.dh_embarque) BETWEEN 10 AND 15 THEN '02_COMERCIAL_TARDE'
        WHEN EXTRACT(HOUR FROM c.dh_embarque) BETWEEN 16 AND 19 THEN '03_PICO_NOITE'
        WHEN EXTRACT(HOUR FROM c.dh_embarque) BETWEEN 20 AND 23 THEN '04_NOTURNO'
        ELSE '05_MADRUGADA'
    END AS turno_viagem,
    -- Enriquecimento Geográfico (Embarque / Desembarque)
    COALESCE(ze.distrito, 'NAO_INFORMADO') AS distrito_embarque,
    COALESCE(ze.nome_zona, 'Zona Não Identificada') AS nome_zona_embarque,
    COALESCE(zd.distrito, 'NAO_INFORMADO') AS distrito_desembarque,
    COALESCE(zd.nome_zona, 'Zona Não Identificada') AS nome_zona_desembarque,
    CASE
        WHEN ze.zona_servico = 'AIRPORTS' OR zd.zona_servico = 'AIRPORTS' THEN 'CONEXAO_AEROPORTO'
        WHEN ze.distrito = zd.distrito THEN 'INTRA_DISTRITO'
        ELSE 'INTER_DISTRITO'
    END AS tipo_rota,
    -- Métricas Físicas e Financeiras Tratadas
    c.qtd_passageiros,
    c.distancia_milhas,
    c.distancia_km,
    c.duracao_minutos,
    ROUND(
        CASE
            WHEN c.duracao_minutos > 0.5 THEN LEAST(c.distancia_km / (c.duracao_minutos / 60.0), 130.00)
            ELSE 0.00
        END,
        2
    ) AS velocidade_media_kmh,
    c.valor_tarifa_base,
    c.valor_extras_total,
    c.valor_gorjeta,
    ROUND(
        CASE
            WHEN c.valor_tarifa_base > 0 THEN (c.valor_gorjeta / c.valor_tarifa_base) * 100.0
            ELSE 0.00
        END,
        2
    ) AS pct_gorjeta,
    c.valor_pedagio,
    c.valor_taxa_congestionamento,
    c.valor_taxa_aeroporto,
    c.valor_total_corrida
FROM corridas_saneadas c
LEFT JOIN {{ ref('int_zonas_tratadas') }} ze
    ON c.id_zona_embarque = ze.id_zona
LEFT JOIN {{ ref('int_zonas_tratadas') }} zd
    ON c.id_zona_desembarque = zd.id_zona
