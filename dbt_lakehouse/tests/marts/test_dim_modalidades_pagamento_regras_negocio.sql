-- Teste de regra de negócio de dim_modalidades_pagamento: todas as modalidades precisam ter descrição
SELECT
    id_dim_modalidades_pagamento,
    descricao_pagamento
FROM {{ ref('dim_modalidades_pagamento') }}
WHERE TRIM(COALESCE(descricao_pagamento, '')) = ''
