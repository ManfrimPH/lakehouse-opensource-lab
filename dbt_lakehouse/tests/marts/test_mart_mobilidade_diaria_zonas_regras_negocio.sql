-- Teste de regra de negócio de mart_mobilidade_diaria_zonas: corridas > 0 e faturamento > 0
SELECT
    chave,
    id_dim_zonas,
    qtd_corridas,
    faturamento_bruto_usd,
    ticket_medio_usd
FROM {{ ref('mart_mobilidade_diaria_zonas') }}
WHERE qtd_corridas <= 0
   OR faturamento_bruto_usd <= 0
   OR ticket_medio_usd <= 0
