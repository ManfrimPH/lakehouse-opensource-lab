-- Teste de regra de negócio de dim_clientes: LTV e Ticket Médio nunca podem ser negativos
SELECT
    id_dim_clientes,
    nome_cliente,
    ltv_cliente,
    ticket_medio
FROM {{ ref('dim_clientes') }}
WHERE ltv_cliente < 0 OR ticket_medio < 0
