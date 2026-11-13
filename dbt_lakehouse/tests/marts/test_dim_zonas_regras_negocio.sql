-- Teste de regra de negócio de dim_zonas: embarques, desembarques e faturamento nunca negativos
SELECT
    id_dim_zonas,
    nome_zona,
    total_embarques,
    total_desembarques,
    faturamento_origem_usd
FROM {{ ref('dim_zonas') }}
WHERE total_embarques < 0 OR total_desembarques < 0 OR faturamento_origem_usd < 0
