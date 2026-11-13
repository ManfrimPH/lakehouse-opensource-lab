-- Camada Marts (models/marts): Dimensão Modalidades de Pagamento NYC TLC (PK MD5: id_dim_modalidades_pagamento)
{{ config(materialized='table') }}

WITH dominio_pagamentos AS (
    SELECT 1 AS cod_pagamento, 'CARTAO_CREDITO'      AS descricao_pagamento, 'DIGITAL_RASTREAVEL' AS categoria_recebimento, TRUE  AS ind_aceita_gorjeta_app UNION ALL
    SELECT 2 AS cod_pagamento, 'DINHEIRO'            AS descricao_pagamento, 'FISICO_OU_ISENTO'   AS categoria_recebimento, FALSE AS ind_aceita_gorjeta_app UNION ALL
    SELECT 3 AS cod_pagamento, 'SEM_COBRANCA_GRATIS' AS descricao_pagamento, 'FISICO_OU_ISENTO'   AS categoria_recebimento, FALSE AS ind_aceita_gorjeta_app UNION ALL
    SELECT 4 AS cod_pagamento, 'DISPUTA_CONTESTACAO' AS descricao_pagamento, 'FISICO_OU_ISENTO'   AS categoria_recebimento, FALSE AS ind_aceita_gorjeta_app UNION ALL
    SELECT 5 AS cod_pagamento, 'DESCONHECIDO'        AS descricao_pagamento, 'FISICO_OU_ISENTO'   AS categoria_recebimento, FALSE AS ind_aceita_gorjeta_app UNION ALL
    SELECT 6 AS cod_pagamento, 'CORRIDA_CANCELADA'   AS descricao_pagamento, 'FISICO_OU_ISENTO'   AS categoria_recebimento, FALSE AS ind_aceita_gorjeta_app
)

SELECT
    MD5(CAST(cod_pagamento AS TEXT)) AS id_dim_modalidades_pagamento,
    descricao_pagamento,
    categoria_recebimento,
    ind_aceita_gorjeta_app
FROM dominio_pagamentos
