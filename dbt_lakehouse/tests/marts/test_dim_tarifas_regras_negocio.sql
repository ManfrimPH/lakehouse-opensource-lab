-- Teste de regra de negócio de dim_tarifas: todas as modalidades tarifárias precisam ter nome
SELECT
    id_dim_tarifas,
    nome_tarifa
FROM {{ ref('dim_tarifas') }}
WHERE TRIM(COALESCE(nome_tarifa, '')) = ''
