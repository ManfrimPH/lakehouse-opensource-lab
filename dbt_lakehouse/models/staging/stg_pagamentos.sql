-- Camada Staging (models/staging): Espelho 1:1 da source RAW sem nenhum tratamento (apenas tipagem e rename)
{{ config(materialized='view') }}

SELECT
    CAST(id_pagamento AS INTEGER)             AS id_pagamento,
    CAST(id_pedido AS INTEGER)                AS id_pedido,
    CAST(meio_pagamento AS TEXT)              AS meio_pagamento,
    CAST(parcelas AS INTEGER)                 AS parcelas,
    CAST(taxa_gateway_pct AS NUMERIC(6,2))    AS taxa_gateway_pct,
    CAST(valor_taxa_gateway AS NUMERIC(14,2)) AS valor_taxa_gateway,
    CAST(status_liquidacao AS TEXT)           AS status_liquidacao
FROM {{ source('raw', 'raw_pagamentos') }}
