-- Teste de regra de negócio de mart_faturamento_diario: vendas concluídas sempre com total positivo
SELECT
    chave,
    id_dim_clientes,
    total_vendido,
    receita_liquida_pos_taxa,
    qtd_pedidos
FROM {{ ref('mart_faturamento_diario') }}
WHERE total_vendido <= 0 OR receita_liquida_pos_taxa <= 0 OR qtd_pedidos <= 0
