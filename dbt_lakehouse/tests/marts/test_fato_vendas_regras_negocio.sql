-- Teste de regra de negócio de fato_vendas: valores financeiros nunca podem ser negativos
SELECT
    chave,
    id_dim_clientes,
    valor_bruto,
    desconto,
    valor_liquido
FROM {{ ref('fato_vendas') }}
WHERE valor_liquido < 0 OR valor_bruto < 0 OR desconto < 0
