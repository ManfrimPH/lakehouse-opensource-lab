-- Camada Intermediate (models/intermediate): Tratamento de domínios, limpeza de strings, métricas por zona e chave MD5
{{ config(materialized='view') }}

WITH zonas_limpas AS (
    SELECT
        id_zona,
        MD5(CAST(id_zona AS TEXT)) AS id_dim_zonas,
        CASE
            WHEN UPPER(TRIM(COALESCE(distrito, ''))) IN ('', 'UNKNOWN', 'N/A') THEN 'NAO_INFORMADO'
            ELSE UPPER(TRIM(distrito))
        END AS distrito,
        CASE
            WHEN TRIM(COALESCE(nome_zona, '')) IN ('', 'NV', 'NA', 'Unknown') THEN 'Zona Não Identificada'
            ELSE TRIM(nome_zona)
        END AS nome_zona,
        CASE
            WHEN UPPER(TRIM(COALESCE(zona_servico, ''))) IN ('', 'N/A', 'UNKNOWN') THEN 'NAO_INFORMADO'
            ELSE UPPER(REPLACE(TRIM(zona_servico), ' ', '_'))
        END AS zona_servico
    FROM {{ ref('stg_zonas') }}
),

corridas_validas AS (
    SELECT
        id_zona_embarque,
        id_zona_desembarque,
        COALESCE(valor_total_corrida, 0.00) AS valor_total_corrida
    FROM {{ ref('stg_corridas') }}
    WHERE COALESCE(valor_total_corrida, 0) > 0
      AND COALESCE(valor_tarifa_base, 0) >= 0
      AND COALESCE(distancia_milhas, 0) >= 0
      AND dh_desembarque > dh_embarque
),

embarques_por_zona AS (
    SELECT
        id_zona_embarque AS id_zona,
        COUNT(*) AS total_embarques,
        COALESCE(SUM(valor_total_corrida), 0.00) AS faturamento_origem_usd
    FROM corridas_validas
    GROUP BY id_zona_embarque
),

desembarques_por_zona AS (
    SELECT
        id_zona_desembarque AS id_zona,
        COUNT(*) AS total_desembarques
    FROM corridas_validas
    GROUP BY id_zona_desembarque
)

SELECT
    z.id_dim_zonas,
    z.id_zona,
    z.distrito,
    z.nome_zona,
    z.zona_servico,
    COALESCE(e.total_embarques, 0)              AS total_embarques,
    COALESCE(d.total_desembarques, 0)           AS total_desembarques,
    COALESCE(e.faturamento_origem_usd, 0.00)    AS faturamento_origem_usd,
    CASE
        WHEN COALESCE(e.total_embarques, 0) >= 200 THEN 'HUB_ALTA_DEMANDA'
        WHEN COALESCE(e.total_embarques, 0) >= 20 THEN 'DEMANDA_MEDIA'
        ELSE 'DEMANDA_BAIXA'
    END                                         AS tier_demanda_zona
FROM zonas_limpas z
LEFT JOIN embarques_por_zona e
    ON z.id_zona = e.id_zona
LEFT JOIN desembarques_por_zona d
    ON z.id_zona = d.id_zona
