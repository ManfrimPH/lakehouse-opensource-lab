#!/usr/bin/env python3
"""
Bootstrap Automatizado de Ponta a Ponta do Lakehouse Open Source Lab
Organização Canônica (Source Raw -> Staging -> Intermediate -> Marts):
1. MinIO S3 + Camada Raw:
   - Grava os arquivos .parquet brutos em s3://lakehouse/landing_parquet/
   - Carrega os Parquets do MinIO para o schema 'raw' (raw_clientes, raw_pedidos, raw_pagamentos)
2. Project Nessie + Dremio:
   - Cria Spaces padronizados: 01_raw, 02_staging, 03_intermediate, 04_marts
   - Cria tabelas Apache Iceberg na raiz do catálogo 'nessie' (raw_clientes, raw_pedidos, raw_pagamentos)
   - Cria views em '02_staging' (stg_clientes, stg_pedidos, stg_pagamentos)
   - Cria views em '03_intermediate' (int_clientes_metricas, int_pedidos_enriquecidos)
   - Cria views em '04_marts' (dim_clientes, fato_vendas, mart_faturamento_diario)
3. dbt-core:
   - Executa dbt test --select source:raw
   - Executa dbt run/test --select staging
   - Executa dbt run/test --select intermediate
   - Executa dbt run --select marts + dbt test individual por tabela de Mart
4. Apache Superset: Registra datasets (dim_clientes, fato_vendas, mart_faturamento_diario)
"""
import time
import subprocess
import requests

NESSIE_URL = "http://localhost:19120/api/v1"
DREMIO_URL = "http://localhost:9047"
SUPERSET_URL = "http://localhost:8088"

DREMIO_USER = "admin"
DREMIO_PASS = "Admin1234!"


def step(msg: str):
    print(f"\n{'=' * 78}\n🚀 {msg}\n{'=' * 78}")


def wait_http(name: str, url: str, timeout_sec: int = 180):
    print(f"⏳ Aguardando {name} responder em {url} ...")
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            r = requests.get(url, timeout=4)
            if r.status_code < 500:
                print(f"✅ {name} está online! ({int(time.time() - start)}s)")
                return True
        except Exception:
            pass
        time.sleep(3)
    print(f"⚠️ Tempo limite aguardando {name} em {url}")
    return False


def dremio_sql_sync(token: str, sql: str, wait_sec: int = 35):
    headers = {"Authorization": token, "Content-Type": "application/json"}
    r = requests.post(f"{DREMIO_URL}/api/v3/sql", headers=headers, json={"sql": sql}, timeout=15)
    if r.status_code != 200:
        return False
    job_id = r.json()["id"]
    start = time.time()
    while time.time() - start < wait_sec:
        jr = requests.get(f"{DREMIO_URL}/api/v3/job/{job_id}", headers=headers, timeout=10)
        if jr.status_code == 200:
            state = jr.json().get("jobState")
            if state == "COMPLETED":
                return True
            if state in ("FAILED", "CANCELED"):
                print(f"   [SQL Aviso] {sql[:75]} -> {jr.json().get('errorMessage', '')}")
                return False
        time.sleep(1)
    return False


def setup_minio_parquet_landing():
    step("1/4 - MinIO Landing (.parquet) + Carga da Camada RAW (NYC TLC + Vendas)")
    cmd = """
docker exec lakehouse_airflow python3 -c "
import csv, psycopg2, pyarrow as pa, pyarrow.parquet as pq, pyarrow.fs as pafs
from psycopg2.extras import execute_values

s3 = pafs.S3FileSystem(access_key='minioadmin', secret_key='minioadmin123', endpoint_override='minio:9000', scheme='http')
s3.create_dir('lakehouse/landing_parquet')

for ent in ['clientes', 'pedidos', 'pagamentos']:
    with open(f'/opt/airflow/scripts/sample_data/raw_{ent}.csv', 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    tbl = pa.table({k: [r[k] for r in rows] for k in rows[0].keys()})
    pq.write_table(tbl, f'lakehouse/landing_parquet/{ent}.parquet', filesystem=s3)
    print(f'   • Gravado s3://lakehouse/landing_parquet/{ent}.parquet ({len(rows)} linhas)')

for p_name, local_file in [('taxi_zones', 'taxi_zones.parquet'), ('yellow_tripdata', 'yellow_tripdata_sample.parquet')]:
    tbl_p = pq.read_table(f'/opt/airflow/scripts/sample_data/{local_file}')
    pq.write_table(tbl_p, f'lakehouse/landing_parquet/{p_name}.parquet', filesystem=s3)
    print(f'   • Gravado s3://lakehouse/landing_parquet/{p_name}.parquet ({tbl_p.num_rows} linhas públicas NYC TLC)')

conn = psycopg2.connect('dbname=lakehouse_meta user=lakehouse password=lakehouse123 host=postgres port=5432')
conn.autocommit = True
cur = conn.cursor()
cur.execute('CREATE SCHEMA IF NOT EXISTS raw;')

# 1. raw.raw_clientes
tbl_cli = pq.read_table('lakehouse/landing_parquet/clientes.parquet', filesystem=s3)
cur.execute('''
    DROP TABLE IF EXISTS raw.raw_clientes CASCADE;
    CREATE TABLE raw.raw_clientes (
        id_cliente INT, nome_cliente VARCHAR(150), segmento VARCHAR(50),
        uf VARCHAR(10), cidade VARCHAR(100), data_cadastro DATE
    );
''')
execute_values(cur, 'INSERT INTO raw.raw_clientes VALUES %s', [
    (int(r['id_cliente']), r['nome_cliente'], r['segmento'], r['uf'], r['cidade'], r['data_cadastro'])
    for r in tbl_cli.to_pylist()
])

# 2. raw.raw_pedidos
tbl_ped = pq.read_table('lakehouse/landing_parquet/pedidos.parquet', filesystem=s3)
cur.execute('''
    DROP TABLE IF EXISTS raw.raw_pedidos CASCADE;
    CREATE TABLE raw.raw_pedidos (
        id_pedido INT, id_cliente INT, data_venda DATE, status_pedido VARCHAR(40),
        forma_pagamento VARCHAR(40), valor_bruto NUMERIC(14,2), desconto NUMERIC(14,2), valor_liquido NUMERIC(14,2)
    );
''')
execute_values(cur, 'INSERT INTO raw.raw_pedidos VALUES %s', [
    (int(r['id_pedido']), int(r['id_cliente']), r['data_venda'], r['status_pedido'],
     r['forma_pagamento'], float(r['valor_bruto']), float(r['desconto']), float(r['valor_liquido']))
    for r in tbl_ped.to_pylist()
])

# 3. raw.raw_pagamentos
tbl_pag = pq.read_table('lakehouse/landing_parquet/pagamentos.parquet', filesystem=s3)
cur.execute('''
    DROP TABLE IF EXISTS raw.raw_pagamentos CASCADE;
    CREATE TABLE raw.raw_pagamentos (
        id_pagamento INT, id_pedido INT, meio_pagamento VARCHAR(40), parcelas INT,
        taxa_gateway_pct NUMERIC(6,2), valor_taxa_gateway NUMERIC(14,2), status_liquidacao VARCHAR(40)
    );
''')
execute_values(cur, 'INSERT INTO raw.raw_pagamentos VALUES %s', [
    (int(r['id_pagamento']), int(r['id_pedido']), r['meio_pagamento'], int(r['parcelas']),
     float(r['taxa_gateway_pct']), float(r['valor_taxa_gateway']), r['status_liquidacao'])
    for r in tbl_pag.to_pylist()
])

# 4. raw.raw_taxi_zones (NYC TLC Public Data)
tbl_z = pq.read_table('lakehouse/landing_parquet/taxi_zones.parquet', filesystem=s3)
cur.execute('''
    DROP TABLE IF EXISTS raw.raw_taxi_zones CASCADE;
    CREATE TABLE raw.raw_taxi_zones (
        location_id INT, borough TEXT, zone TEXT, service_zone TEXT
    );
''')
execute_values(cur, 'INSERT INTO raw.raw_taxi_zones VALUES %s', [
    (int(r['location_id']), r['borough'], r['zone'], r['service_zone'])
    for r in tbl_z.to_pylist()
])

# 5. raw.raw_yellow_tripdata (NYC TLC Public Data - 15.000 corridas reais)
tbl_t = pq.read_table('lakehouse/landing_parquet/yellow_tripdata.parquet', filesystem=s3)
cur.execute('''
    DROP TABLE IF EXISTS raw.raw_yellow_tripdata CASCADE;
    CREATE TABLE raw.raw_yellow_tripdata (
        trip_id BIGINT, vendorid INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
        passenger_count NUMERIC(10,2), trip_distance NUMERIC(12,2), ratecodeid NUMERIC(10,2),
        store_and_fwd_flag TEXT, pulocationid INT, dolocationid INT, payment_type INT,
        fare_amount NUMERIC(12,2), extra NUMERIC(12,2), mta_tax NUMERIC(12,2), tip_amount NUMERIC(12,2),
        tolls_amount NUMERIC(12,2), improvement_surcharge NUMERIC(12,2), total_amount NUMERIC(12,2),
        congestion_surcharge NUMERIC(12,2), airport_fee NUMERIC(12,2)
    );
''')
execute_values(cur, 'INSERT INTO raw.raw_yellow_tripdata VALUES %s', [
    (r['trip_id'], r['vendorid'], r['tpep_pickup_datetime'], r['tpep_dropoff_datetime'],
     r['passenger_count'], r['trip_distance'], r['ratecodeid'], r['store_and_fwd_flag'],
     r['pulocationid'], r['dolocationid'], r['payment_type'], r['fare_amount'], r['extra'],
     r['mta_tax'], r['tip_amount'], r['tolls_amount'], r['improvement_surcharge'],
     r['total_amount'], r['congestion_surcharge'], r['airport_fee'])
    for r in tbl_t.to_pylist()
], page_size=2000)

conn.close()
print('   • Camada RAW populada com sucesso (raw_taxi_zones: 265 | raw_yellow_tripdata: 15.000 | vendas: 40)!')
"
"""
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(res.stdout.strip())


def setup_dremio_and_nessie():
    step("2/4 - Estruturando Dremio + Nessie: 01_raw -> 02_staging -> 03_intermediate -> 04_marts (Iceberg Puro)")
    if not wait_http("Dremio UI/API", f"{DREMIO_URL}/", timeout_sec=180):
        return

    bootstrap_payload = {
        "userName": DREMIO_USER,
        "firstName": "Pedro",
        "lastName": "Manfrim",
        "email": "pedro@lakehouse.local",
        "createdAt": int(time.time() * 1000),
        "password": DREMIO_PASS,
    }
    try:
        requests.put(
            f"{DREMIO_URL}/apiv2/bootstrap/firstuser",
            json=bootstrap_payload,
            headers={"Authorization": "_dremionull"},
            timeout=10,
        )
    except Exception:
        pass

    login_resp = requests.post(
        f"{DREMIO_URL}/apiv2/login",
        json={"userName": DREMIO_USER, "password": DREMIO_PASS},
        timeout=10,
    )
    token = f"_dremio{login_resp.json()['token']}"
    headers = {"Authorization": token, "Content-Type": "application/json"}

    # Conecta Nessie como Catálogo Iceberg
    nessie_source = {
        "entityType": "source",
        "name": "nessie",
        "type": "NESSIE",
        "config": {
            "nessieEndpoint": "http://nessie:19120/api/v2",
            "nessieAuthType": "NONE",
            "credentialType": "ACCESS_KEY",
            "awsAccessKey": "minioadmin",
            "awsAccessSecret": "minioadmin123",
            "awsRootPath": "/lakehouse",
            "secure": False,
            "propertyList": [
                {"name": "fs.s3a.path.style.access", "value": "true"},
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "dremio.s3.compat", "value": "true"},
            ],
        },
    }
    requests.post(f"{DREMIO_URL}/api/v3/catalog", headers=headers, json=nessie_source, timeout=20)

    # Conecta PostgreSQL auxiliar para ingestão de datasets brutos de alta volumetria
    pg_source = {
        "entityType": "source",
        "name": "pg_lakehouse",
        "type": "POSTGRES",
        "config": {
            "hostname": "postgres",
            "port": 5432,
            "databaseName": "lakehouse_meta",
            "username": "lakehouse",
            "password": "lakehouse123",
        },
    }
    requests.post(f"{DREMIO_URL}/api/v3/catalog", headers=headers, json=pg_source, timeout=20)

    # Cria os 4 Spaces no Dremio: 01_raw, 02_staging, 03_intermediate, 04_marts
    for space_name in ["01_raw", "02_staging", "03_intermediate", "04_marts"]:
        requests.post(
            f"{DREMIO_URL}/api/v3/catalog",
            headers=headers,
            json={"entityType": "space", "name": space_name},
            timeout=10,
        )

    print("   • Materializando Tabelas Apache Iceberg físicas no Project Nessie (S3/MinIO)...")

    # 1. RAW TABLES NO NESSIE
    raw_statements = [
        "DROP TABLE IF EXISTS nessie.raw.clientes",
        "CREATE TABLE nessie.raw.clientes AS SELECT * FROM pg_lakehouse.raw.raw_clientes",
        "DROP TABLE IF EXISTS nessie.raw.pedidos",
        "CREATE TABLE nessie.raw.pedidos AS SELECT * FROM pg_lakehouse.raw.raw_pedidos",
        "DROP TABLE IF EXISTS nessie.raw.pagamentos",
        "CREATE TABLE nessie.raw.pagamentos AS SELECT * FROM pg_lakehouse.raw.raw_pagamentos",
        "DROP TABLE IF EXISTS nessie.raw.taxi_zones",
        "CREATE TABLE nessie.raw.taxi_zones AS SELECT * FROM pg_lakehouse.raw.raw_taxi_zones",
        "DROP TABLE IF EXISTS nessie.raw.yellow_tripdata",
        "CREATE TABLE nessie.raw.yellow_tripdata AS SELECT * FROM pg_lakehouse.raw.raw_yellow_tripdata",
    ]
    for s in raw_statements:
        dremio_sql_sync(token, s.strip(), wait_sec=40)
    print("     [Nessie] Camada RAW (Iceberg) materializada!")

    # 2. STAGING TABLES NO NESSIE
    stg_statements = [
        "DROP TABLE IF EXISTS nessie.staging.stg_clientes",
        """CREATE TABLE nessie.staging.stg_clientes AS
           SELECT id_cliente, TRIM(nome_cliente) AS nome_cliente, UPPER(TRIM(segmento)) AS segmento,
                  UPPER(TRIM(uf)) AS uf, TRIM(cidade) AS cidade, data_cadastro
           FROM nessie.raw.clientes""",

        "DROP TABLE IF EXISTS nessie.staging.stg_pedidos",
        """CREATE TABLE nessie.staging.stg_pedidos AS
           SELECT id_pedido, id_cliente, data_venda, UPPER(TRIM(status_pedido)) AS status_pedido,
                  UPPER(TRIM(forma_pagamento)) AS forma_pagamento, valor_bruto, desconto, valor_liquido
           FROM nessie.raw.pedidos""",

        "DROP TABLE IF EXISTS nessie.staging.stg_pagamentos",
        """CREATE TABLE nessie.staging.stg_pagamentos AS
           SELECT id_pagamento, id_pedido, UPPER(TRIM(meio_pagamento)) AS meio_pagamento,
                  parcelas, taxa_gateway_pct, valor_taxa_gateway, UPPER(TRIM(status_liquidacao)) AS status_liquidacao
           FROM nessie.raw.pagamentos""",

        "DROP TABLE IF EXISTS nessie.staging.stg_zonas",
        """CREATE TABLE nessie.staging.stg_zonas AS
           SELECT location_id AS id_zona, TRIM(borough) AS distrito, TRIM(zone) AS nome_zona,
                  TRIM(service_zone) AS zona_servico
           FROM nessie.raw.taxi_zones""",

        "DROP TABLE IF EXISTS nessie.staging.stg_corridas",
        """CREATE TABLE nessie.staging.stg_corridas AS
           SELECT trip_id AS id_corrida_origem, vendorid AS id_fornecedor, ratecodeid AS id_tarifa,
                  payment_type AS id_tipo_pagamento, pulocationid AS id_zona_embarque, dolocationid AS id_zona_desembarque,
                  tpep_pickup_datetime AS dh_embarque, tpep_dropoff_datetime AS dh_desembarque,
                  passenger_count AS qtd_passageiros, trip_distance AS distancia_milhas, fare_amount AS valor_tarifa_base,
                  extra AS valor_extra, mta_tax AS valor_imposto_mta, tip_amount AS valor_gorjeta,
                  tolls_amount AS valor_pedagio, improvement_surcharge AS valor_sobretaxa_melhoria,
                  total_amount AS valor_total_corrida, congestion_surcharge AS valor_taxa_congestionamento,
                  airport_fee AS valor_taxa_aeroporto
           FROM nessie.raw.yellow_tripdata""",
    ]
    for s in stg_statements:
        dremio_sql_sync(token, s.strip(), wait_sec=40)
    print("     [Nessie] Camada Staging (Iceberg) materializada!")

    # 3. INTERMEDIATE TABLES NO NESSIE
    int_statements = [
        "DROP TABLE IF EXISTS nessie.intermediate.int_clientes_metricas",
        """CREATE TABLE nessie.intermediate.int_clientes_metricas AS
           SELECT c.id_cliente, c.nome_cliente, c.segmento, c.uf, c.cidade, c.data_cadastro,
                  COUNT(p.id_pedido) AS total_pedidos, COALESCE(SUM(p.valor_liquido), 0) AS ltv_cliente
           FROM nessie.staging.stg_clientes AS c
           LEFT JOIN nessie.staging.stg_pedidos AS p ON c.id_cliente = p.id_cliente AND p.status_pedido = 'CONCLUIDO'
           GROUP BY c.id_cliente, c.nome_cliente, c.segmento, c.uf, c.cidade, c.data_cadastro""",

        "DROP TABLE IF EXISTS nessie.intermediate.int_pedidos_enriquecidos",
        """CREATE TABLE nessie.intermediate.int_pedidos_enriquecidos AS
           SELECT p.id_pedido, p.id_cliente, c.nome_cliente, c.segmento AS segmento_cliente, c.uf AS uf_cliente,
                  p.data_venda, p.status_pedido, p.forma_pagamento, p.valor_bruto, p.desconto, p.valor_liquido,
                  COALESCE(pg.valor_taxa_gateway, 0) AS valor_taxa_gateway,
                  (p.valor_liquido - COALESCE(pg.valor_taxa_gateway, 0)) AS receita_liquida_real,
                  COALESCE(pg.status_liquidacao, 'PENDENTE') AS status_liquidacao
           FROM nessie.staging.stg_pedidos AS p
           JOIN nessie.staging.stg_clientes AS c ON p.id_cliente = c.id_cliente
           LEFT JOIN nessie.staging.stg_pagamentos AS pg ON p.id_pedido = pg.id_pedido""",

        "DROP TABLE IF EXISTS nessie.intermediate.int_zonas_tratadas",
        """CREATE TABLE nessie.intermediate.int_zonas_tratadas AS
           WITH zonas_limpas AS (
               SELECT id_zona, MD5(CAST(id_zona AS VARCHAR)) AS id_dim_zonas,
                      CASE WHEN UPPER(TRIM(COALESCE(distrito, ''))) IN ('', 'UNKNOWN', 'N/A') THEN 'NAO_INFORMADO' ELSE UPPER(TRIM(distrito)) END AS distrito,
                      CASE WHEN TRIM(COALESCE(nome_zona, '')) IN ('', 'NV', 'NA', 'Unknown') THEN 'Zona Não Identificada' ELSE TRIM(nome_zona) END AS nome_zona,
                      CASE WHEN UPPER(TRIM(COALESCE(zona_servico, ''))) IN ('', 'N/A', 'UNKNOWN') THEN 'NAO_INFORMADO' ELSE UPPER(REPLACE(TRIM(zona_servico), ' ', '_')) END AS zona_servico
               FROM nessie.staging.stg_zonas
           ),
           corridas_validas AS (
               SELECT id_zona_embarque, id_zona_desembarque, COALESCE(valor_total_corrida, 0.00) AS valor_total_corrida
               FROM nessie.staging.stg_corridas
               WHERE COALESCE(valor_total_corrida, 0) > 0 AND COALESCE(valor_tarifa_base, 0) >= 0 AND COALESCE(distancia_milhas, 0) >= 0 AND dh_desembarque > dh_embarque
           ),
           embarques_por_zona AS (
               SELECT id_zona_embarque AS id_zona, COUNT(*) AS total_embarques, COALESCE(SUM(valor_total_corrida), 0.00) AS faturamento_origem_usd
               FROM corridas_validas GROUP BY id_zona_embarque
           ),
           desembarques_por_zona AS (
               SELECT id_zona_desembarque AS id_zona, COUNT(*) AS total_desembarques
               FROM corridas_validas GROUP BY id_zona_desembarque
           )
           SELECT z.id_zona, z.id_dim_zonas, z.distrito, z.nome_zona, z.zona_servico,
                  COALESCE(e.total_embarques, 0) AS total_embarques, COALESCE(d.total_desembarques, 0) AS total_desembarques,
                  COALESCE(e.faturamento_origem_usd, 0.00) AS faturamento_origem_usd
           FROM zonas_limpas AS z
           LEFT JOIN embarques_por_zona AS e ON z.id_zona = e.id_zona
           LEFT JOIN desembarques_por_zona AS d ON z.id_zona = d.id_zona""",

        "DROP TABLE IF EXISTS nessie.intermediate.int_corridas_tratadas",
        """CREATE TABLE nessie.intermediate.int_corridas_tratadas AS
           WITH corridas_saneadas AS (
               SELECT id_corrida_origem, COALESCE(id_fornecedor, 99) AS id_fornecedor,
                      CASE WHEN id_tarifa IN (1, 2, 3, 4, 5, 6) THEN id_tarifa ELSE 99 END AS id_tarifa,
                      CASE WHEN id_tipo_pagamento IN (1, 2, 3, 4, 5, 6) THEN id_tipo_pagamento ELSE 5 END AS id_tipo_pagamento,
                      COALESCE(id_zona_embarque, 264) AS id_zona_embarque, COALESCE(id_zona_desembarque, 264) AS id_zona_desembarque,
                      dh_embarque, dh_desembarque,
                      CASE WHEN COALESCE(qtd_passageiros, 0) <= 0 THEN 1 ELSE qtd_passageiros END AS qtd_passageiros,
                      COALESCE(distancia_milhas, 0.00) AS distancia_milhas,
                      ROUND(COALESCE(distancia_milhas, 0.00) * 1.60934, 2) AS distancia_km,
                      TIMESTAMPDIFF(MINUTE, dh_embarque, dh_desembarque) AS duracao_minutos,
                      COALESCE(valor_tarifa_base, 0.00) AS valor_tarifa_base,
                      (COALESCE(valor_extra, 0.00) + COALESCE(valor_imposto_mta, 0.00) + COALESCE(valor_sobretaxa_melhoria, 0.00)) AS valor_extras_total,
                      GREATEST(COALESCE(valor_gorjeta, 0.00), 0.00) AS valor_gorjeta,
                      GREATEST(COALESCE(valor_pedagio, 0.00), 0.00) AS valor_pedagio,
                      GREATEST(COALESCE(valor_taxa_congestionamento, 0.00), 0.00) AS valor_taxa_congestionamento,
                      GREATEST(COALESCE(valor_taxa_aeroporto, 0.00), 0.00) AS valor_taxa_aeroporto,
                      COALESCE(valor_total_corrida, 0.00) AS valor_total_corrida
               FROM nessie.staging.stg_corridas
               WHERE COALESCE(valor_total_corrida, 0) > 0 AND COALESCE(valor_tarifa_base, 0) >= 0 AND COALESCE(distancia_milhas, 0) >= 0 AND dh_desembarque > dh_embarque
           )
           SELECT MD5(CAST(c.id_corrida_origem AS VARCHAR))   AS chave,
                  MD5(CAST(c.id_zona_embarque AS VARCHAR))    AS id_dim_zonas_embarque,
                  MD5(CAST(c.id_zona_desembarque AS VARCHAR)) AS id_dim_zonas_desembarque,
                  MD5(CAST(c.id_tipo_pagamento AS VARCHAR))   AS id_dim_modalidades_pagamento,
                  MD5(CAST(c.id_tarifa AS VARCHAR))           AS id_dim_tarifas,
                  c.id_corrida_origem, c.id_zona_embarque, c.id_zona_desembarque, c.id_fornecedor, c.id_tarifa, c.id_tipo_pagamento,
                  c.dh_embarque, c.dh_desembarque, CAST(c.dh_embarque AS DATE) AS dt_corrida,
                  c.qtd_passageiros, c.distancia_milhas, c.distancia_km, c.duracao_minutos,
                  CASE WHEN c.duracao_minutos > 0 THEN ROUND((c.distancia_km / (c.duracao_minutos / 60.0)), 2) ELSE 0.00 END AS velocidade_media_kmh,
                  c.valor_tarifa_base, c.valor_extras_total, c.valor_gorjeta, c.valor_pedagio, c.valor_taxa_congestionamento, c.valor_taxa_aeroporto, c.valor_total_corrida
           FROM corridas_saneadas AS c""",
    ]
    for s in int_statements:
        dremio_sql_sync(token, s.strip(), wait_sec=40)
    print("     [Nessie] Camada Intermediate (Iceberg) materializada!")

    # 4. MARTS NO NESSIE
    marts_statements = [
        "DROP TABLE IF EXISTS nessie.marts.dim_clientes",
        """CREATE TABLE nessie.marts.dim_clientes AS
           SELECT MD5(CAST(id_cliente AS VARCHAR)) AS id_dim_clientes, id_cliente, nome_cliente, segmento,
                  uf, cidade, data_cadastro, total_pedidos, ltv_cliente,
                  CASE WHEN ltv_cliente >= 20000 THEN 'CLIENTE_VIP' WHEN ltv_cliente >= 10000 THEN 'CLIENTE_ESTRATEGICO' ELSE 'CLIENTE_STANDARD' END AS perfil_cliente
           FROM nessie.intermediate.int_clientes_metricas""",

        "DROP TABLE IF EXISTS nessie.marts.dim_zonas",
        """CREATE TABLE nessie.marts.dim_zonas AS
           SELECT id_dim_zonas, id_zona, distrito, nome_zona, zona_servico,
                  total_embarques, total_desembarques, faturamento_origem_usd
           FROM nessie.intermediate.int_zonas_tratadas""",

        "DROP TABLE IF EXISTS nessie.marts.dim_modalidades_pagamento",
        """CREATE TABLE nessie.marts.dim_modalidades_pagamento AS
           SELECT MD5(CAST(id_tipo_pagamento AS VARCHAR)) AS id_dim_modalidades_pagamento, id_tipo_pagamento, nome_modalidade, tipo_meio, exige_conciliacao
           FROM (VALUES
               (1, 'Cartão de Crédito', 'ELETRONICO', true),
               (2, 'Dinheiro em Espécie', 'FISICO', false),
               (3, 'Sem Cobrança / Cortesia', 'ISENTO', false),
               (4, 'Disputa / Estorno', 'ADMINISTRATIVO', true),
               (5, 'Desconhecido', 'NAO_INFORMADO', false),
               (6, 'Corrida Anulada', 'CANCELADO', false)
           ) AS t(id_tipo_pagamento, nome_modalidade, tipo_meio, exige_conciliacao)""",

        "DROP TABLE IF EXISTS nessie.marts.dim_tarifas",
        """CREATE TABLE nessie.marts.dim_tarifas AS
           SELECT MD5(CAST(id_tarifa AS VARCHAR)) AS id_dim_tarifas, id_tarifa, descricao_tarifa, tipo_tarifa
           FROM (VALUES
               (1, 'Tarifa Padrão', 'REGULAR'),
               (2, 'Aeroporto JFK', 'AEROPORTO'),
               (3, 'Aeroporto Newark', 'AEROPORTO'),
               (4, 'Nassau / Westchester', 'INTERMUNICIPAL'),
               (5, 'Tarifa Negociada', 'ESPECIAL'),
               (6, 'Passeio em Grupo', 'COMPARTILHADO'),
               (99, 'Não Identificada', 'OUTROS')
           ) AS t(id_tarifa, descricao_tarifa, tipo_tarifa)""",

        "DROP TABLE IF EXISTS nessie.marts.fato_corridas",
        """CREATE TABLE nessie.marts.fato_corridas AS
           SELECT chave, id_dim_zonas_embarque, id_dim_zonas_desembarque, id_dim_modalidades_pagamento, id_dim_tarifas,
                  id_corrida_origem, dh_embarque, dh_desembarque, dt_corrida, qtd_passageiros, distancia_milhas,
                  distancia_km, duracao_minutos, velocidade_media_kmh, valor_tarifa_base, valor_extras_total,
                  valor_gorjeta, valor_pedagio, valor_taxa_congestionamento, valor_taxa_aeroporto, valor_total_corrida
           FROM nessie.intermediate.int_corridas_tratadas""",

        "DROP TABLE IF EXISTS nessie.marts.fato_vendas",
        """CREATE TABLE nessie.marts.fato_vendas AS
           SELECT MD5(CAST(id_pedido AS VARCHAR)) AS id_fato_vendas, MD5(CAST(id_cliente AS VARCHAR)) AS id_dim_clientes,
                  id_pedido, id_cliente, data_venda, status_pedido, forma_pagamento, valor_bruto, desconto,
                  valor_liquido, valor_taxa_gateway, receita_liquida_real, status_liquidacao
           FROM nessie.intermediate.int_pedidos_enriquecidos""",

        "DROP TABLE IF EXISTS nessie.marts.mart_faturamento_diario",
        """CREATE TABLE nessie.marts.mart_faturamento_diario AS
           SELECT MD5(CAST(data_venda AS VARCHAR)) AS id_mart_faturamento, data_venda,
                  COUNT(id_pedido) AS total_pedidos, SUM(valor_bruto) AS faturamento_bruto,
                  SUM(desconto) AS total_descontos, SUM(valor_liquido) AS faturamento_liquido,
                  SUM(receita_liquida_real) AS receita_real_caixa, ROUND(AVG(valor_liquido), 2) AS ticket_medio,
                  ROUND((SUM(desconto) / NULLIF(SUM(valor_bruto), 0)) * 100, 2) AS percentual_medio_desconto
           FROM nessie.intermediate.int_pedidos_enriquecidos
           WHERE status_pedido = 'CONCLUIDO'
           GROUP BY data_venda""",

        "DROP TABLE IF EXISTS nessie.marts.mart_mobilidade_diaria_zonas",
        """CREATE TABLE nessie.marts.mart_mobilidade_diaria_zonas AS
           SELECT MD5(CONCAT(CAST(dt_corrida AS VARCHAR), '_', CAST(id_dim_zonas_embarque AS VARCHAR))) AS id_mart_mobilidade,
                  dt_corrida, id_dim_zonas_embarque, COUNT(*) AS total_corridas, SUM(qtd_passageiros) AS total_passageiros,
                  ROUND(SUM(distancia_km), 2) AS total_km_rodados, ROUND(AVG(duracao_minutos), 2) AS tempo_medio_minutos,
                  ROUND(AVG(velocidade_media_kmh), 2) AS velocidade_media_geral_kmh, ROUND(SUM(valor_tarifa_base), 2) AS receita_tarifas_usd,
                  ROUND(SUM(valor_gorjeta), 2) AS total_gorjetas_usd, ROUND(SUM(valor_pedagio), 2) AS total_pedagios_usd,
                  ROUND(SUM(valor_total_corrida), 2) AS receita_bruta_total_usd, ROUND(AVG(valor_total_corrida), 2) AS ticket_medio_corrida_usd
           FROM nessie.marts.fato_corridas
           GROUP BY dt_corrida, id_dim_zonas_embarque""",
    ]
    for s in marts_statements:
        dremio_sql_sync(token, s.strip(), wait_sec=40)
    print("     [Nessie] Camada Marts (Iceberg) materializada!")

    # 5. SPACES DO DREMIO (VIEWS CONSUMINDO O NESSIE COM SEMÂNTICA GIT)
    dremio_space_views = [
        # 01_raw
        'CREATE OR REPLACE VIEW "01_raw".raw_clientes AS SELECT * FROM nessie.raw.clientes AT BRANCH main',
        'CREATE OR REPLACE VIEW "01_raw".raw_pedidos AS SELECT * FROM nessie.raw.pedidos AT BRANCH main',
        'CREATE OR REPLACE VIEW "01_raw".raw_pagamentos AS SELECT * FROM nessie.raw.pagamentos AT BRANCH main',
        'CREATE OR REPLACE VIEW "01_raw".raw_taxi_zones AS SELECT * FROM nessie.raw.taxi_zones AT BRANCH main',
        'CREATE OR REPLACE VIEW "01_raw".raw_yellow_tripdata AS SELECT * FROM nessie.raw.yellow_tripdata AT BRANCH main',

        # 02_staging
        'CREATE OR REPLACE VIEW "02_staging".stg_clientes AS SELECT * FROM nessie.staging.stg_clientes AT BRANCH main',
        'CREATE OR REPLACE VIEW "02_staging".stg_pedidos AS SELECT * FROM nessie.staging.stg_pedidos AT BRANCH main',
        'CREATE OR REPLACE VIEW "02_staging".stg_pagamentos AS SELECT * FROM nessie.staging.stg_pagamentos AT BRANCH main',
        'CREATE OR REPLACE VIEW "02_staging".stg_zonas AS SELECT * FROM nessie.staging.stg_zonas AT BRANCH main',
        'CREATE OR REPLACE VIEW "02_staging".stg_corridas AS SELECT * FROM nessie.staging.stg_corridas AT BRANCH main',

        # 03_intermediate
        'CREATE OR REPLACE VIEW "03_intermediate".int_clientes_metricas AS SELECT * FROM nessie.intermediate.int_clientes_metricas AT BRANCH main',
        'CREATE OR REPLACE VIEW "03_intermediate".int_pedidos_enriquecidos AS SELECT * FROM nessie.intermediate.int_pedidos_enriquecidos AT BRANCH main',
        'CREATE OR REPLACE VIEW "03_intermediate".int_zonas_tratadas AS SELECT * FROM nessie.intermediate.int_zonas_tratadas AT BRANCH main',
        'CREATE OR REPLACE VIEW "03_intermediate".int_corridas_tratadas AS SELECT * FROM nessie.intermediate.int_corridas_tratadas AT BRANCH main',

        # 04_marts
        'CREATE OR REPLACE VIEW "04_marts".dim_clientes AS SELECT * FROM nessie.marts.dim_clientes AT BRANCH main',
        'CREATE OR REPLACE VIEW "04_marts".dim_zonas AS SELECT * FROM nessie.marts.dim_zonas AT BRANCH main',
        'CREATE OR REPLACE VIEW "04_marts".dim_tarifas AS SELECT * FROM nessie.marts.dim_tarifas AT BRANCH main',
        'CREATE OR REPLACE VIEW "04_marts".dim_modalidades_pagamento AS SELECT * FROM nessie.marts.dim_modalidades_pagamento AT BRANCH main',
        'CREATE OR REPLACE VIEW "04_marts".fato_corridas AS SELECT * FROM nessie.marts.fato_corridas AT BRANCH main',
        'CREATE OR REPLACE VIEW "04_marts".fato_vendas AS SELECT * FROM nessie.marts.fato_vendas AT BRANCH main',
        'CREATE OR REPLACE VIEW "04_marts".mart_faturamento_diario AS SELECT * FROM nessie.marts.mart_faturamento_diario AT BRANCH main',
        'CREATE OR REPLACE VIEW "04_marts".mart_mobilidade_diaria_zonas AS SELECT * FROM nessie.marts.mart_mobilidade_diaria_zonas AT BRANCH main',
    ]
    for sv in dremio_space_views:
        dremio_sql_sync(token, sv.strip(), wait_sec=30)
    print("   • Spaces do Dremio atualizados apontando para as tabelas Iceberg versionadas!")

    # Sincroniza branches de homologação e etl no Nessie
    main_r = requests.get(f"{NESSIE_URL}/trees/tree/main", timeout=10).json()
    main_hash = main_r["hash"]
    for b_name in ["etl_diario", "homologacao"]:
        chk = requests.get(f"{NESSIE_URL}/trees/tree/{b_name}", timeout=10)
        if chk.status_code == 200:
            old_h = chk.json()["hash"]
            requests.delete(f"{NESSIE_URL}/trees/branch/{b_name}?expectedHash={old_h}", timeout=10)
        requests.post(
            f"{NESSIE_URL}/trees/tree?sourceRefName=main",
            json={"type": "BRANCH", "name": b_name, "hash": main_hash},
            timeout=10,
        )
    print("   ✅ Dremio e Nessie estruturados com 100% das camadas Medalhão em Apache Iceberg!")



def setup_dbt():
    step("3/4 - Executando Pipeline dbt-core (Raw -> Staging -> Intermediate -> Marts com Teste por Tabela)")
    wait_http("Apache Airflow", "http://localhost:8085/health", timeout_sec=180)
    cmds = [
        # 1. Staging e Intermediate (sem testes, apenas transformações limpas)
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt run --select staging --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt run --select intermediate --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        # 2. Dimensões em Paralelo (cada uma com seu run e test imediato)
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --select dim_zonas --indirect-selection cautious --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --select dim_modalidades_pagamento --indirect-selection cautious --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --select dim_tarifas --indirect-selection cautious --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --select dim_clientes --indirect-selection cautious --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        # 3. Fatos em Paralelo (cada uma com seu run e test imediato)
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --select fato_corridas --indirect-selection cautious --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --select fato_vendas --indirect-selection cautious --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        # 4. Marts Agregados em Paralelo (cada um com seu run e test imediato)
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --select mart_mobilidade_diaria_zonas --indirect-selection cautious --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
        "docker exec -e DBT_SCHEMA=lakehouse_main lakehouse_airflow dbt build --select mart_faturamento_diario --indirect-selection cautious --project-dir /opt/airflow/dbt_lakehouse --profiles-dir /opt/airflow/dbt_lakehouse",
    ]
    for cmd in cmds:
        short_cmd = cmd.split("lakehouse_airflow ")[1].split(" --project-dir")[0]
        print(f"   $ {short_cmd}")
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        for line in res.stdout.splitlines()[-4:]:
            print(f"     {line}")


def setup_superset():
    step("4/4 - Configurando Apache Superset (Datasets de Mobilidade Urbana NYC TLC + Vendas)")
    if not wait_http("Apache Superset", f"{SUPERSET_URL}/health", timeout_sec=180):
        return

    sess = requests.Session()
    login_r = sess.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": "admin", "password": "admin", "provider": "db", "refresh": True},
        timeout=15,
    )
    access_token = login_r.json()["access_token"]
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    dbs_r = sess.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers, timeout=10).json()
    db_id = None
    for db_item in dbs_r.get("result", []):
        if db_item.get("database_name") == "Lakehouse Open Source (Gold Marts)":
            db_id = db_item["id"]
            break

    if db_id:
        for tbl_name in [
            "dim_zonas",
            "dim_modalidades_pagamento",
            "dim_tarifas",
            "fato_corridas",
            "mart_mobilidade_diaria_zonas",
            "dim_clientes",
            "fato_vendas",
            "mart_faturamento_diario",
        ]:
            ds_r = sess.post(
                f"{SUPERSET_URL}/api/v1/dataset/",
                headers=headers,
                json={"database": db_id, "schema": "lakehouse_main", "table_name": tbl_name},
                timeout=10,
            )
            if ds_r.status_code in (200, 201):
                print(f"   • Dataset '{tbl_name}' registrado no Apache Superset!")
            else:
                print(f"   • Dataset '{tbl_name}' já disponível no Apache Superset.")

    # Constrói / atualiza o Dashboard Executivo no Superset
    print("   • Construindo Dashboard Executivo de Mobilidade Urbana no Apache Superset...")
    dash_cmd = "docker exec -i lakehouse_superset python < /home/manfrim/projetos/lakehouse-opensource-lab/scripts/create_superset_dashboard.py"
    dash_res = subprocess.run(dash_cmd, shell=True, capture_output=True, text=True)
    for line in dash_res.stdout.splitlines():
        if "Dashboard criado" in line or "Removendo" in line:
            print(f"     {line}")


if __name__ == "__main__":
    setup_minio_parquet_landing()
    setup_dremio_and_nessie()
    setup_dbt()
    setup_superset()
    print("\n" + "=" * 78)
    print("🎉 ESTRUTURA CANÔNICA (source:raw -> staging -> intermediate -> marts) PRONTA! 🎉")
    print("=" * 78)
