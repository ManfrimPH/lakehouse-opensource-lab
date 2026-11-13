-- Teste de regra de negócio de fato_corridas: valor total positivo, duração > 0 e distância >= 0
SELECT
    chave,
    id_dim_zonas_embarque,
    valor_total_corrida,
    duracao_minutos,
    distancia_km
FROM {{ ref('fato_corridas') }}
WHERE valor_total_corrida <= 0
   OR duracao_minutos <= 0
   OR distancia_km < 0
   OR dh_desembarque <= dh_embarque
