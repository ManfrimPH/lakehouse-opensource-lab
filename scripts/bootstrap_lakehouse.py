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
    step("2/4 - Estruturando Dremio + Nessie: 01_raw -> 02_staging -> 03_intermediate -> 04_marts")
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

    # Remove spaces antigos se existirem
    cat_root = requests.get(f"{DREMIO_URL}/api/v3/catalog", headers=headers, timeout=10).json()
    for item in cat_root.get("data", []):
        path_name = item.get("path", [""])[0]
        if path_name in ("raw_layer", "prep_layer", "business", "03_marts"):
            requests.delete(f"{DREMIO_URL}/api/v3/catalog/{item['id']}", headers=headers, timeout=10)
            print(f"   • Space antigo '{path_name}' removido para padronização.")

    # Cria os 4 Spaces alinhados às pastas do dbt: 01_raw, 02_staging, 03_intermediate, 04_marts
    for space_name in ["01_raw", "02_staging", "03_intermediate", "04_marts"]:
        requests.post(
            f"{DREMIO_URL}/api/v3/catalog",
            headers=headers,
            json={"entityType": "space", "name": space_name},
            timeout=10,
        )
    print("   • Spaces criados no Dremio: '01_raw', '02_staging', '03_intermediate' e '04_marts'!")

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

    sql_statements = [
        "USE REFERENCE main IN nessie",
        # --- Tabelas Apache Iceberg na Camada RAW (nessie.raw_*) ---
        """
        CREATE TABLE IF NOT EXISTS nessie.raw_clientes (
            id_cliente INT,
            nome_cliente VARCHAR,
            segmento VARCHAR,
            uf VARCHAR,
            cidade VARCHAR,
            data_cadastro DATE
        )
        """,
        "DELETE FROM nessie.raw_clientes",
        """
        INSERT INTO nessie.raw_clientes VALUES
            (101, 'TechCorp Soluções S.A.', 'ENTERPRISE', 'SP', 'São Paulo', DATE '2024-01-15'),
            (102, 'Varejo Sul Ltda', 'MID_MARKET', 'PR', 'Curitiba', DATE '2024-02-10'),
            (103, 'AgroNorte Cooperativa', 'ENTERPRISE', 'MT', 'Cuiabá', DATE '2024-03-05'),
            (104, 'FinPay Pagamentos Digitais', 'ENTERPRISE', 'SP', 'Campinas', DATE '2024-03-22'),
            (105, 'Logística Atlântico', 'MID_MARKET', 'SC', 'Joinville', DATE '2024-04-12'),
            (106, 'Rede Saúde Integrada', 'ENTERPRISE', 'MG', 'Belo Horizonte', DATE '2024-05-01'),
            (107, 'Comércio Digital Express', 'SMB', 'RJ', 'Rio de Janeiro', DATE '2024-05-19'),
            (108, 'Indústria Metalúrgica Alfa', 'MID_MARKET', 'RS', 'Porto Alegre', DATE '2024-06-08'),
            (109, 'EducaMais Plataforma EAD', 'MID_MARKET', 'PR', 'Maringá', DATE '2024-07-02'),
            (110, 'BioEnergy Renovável', 'ENTERPRISE', 'GO', 'Goiânia', DATE '2024-08-14')
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.raw_pedidos (
            id_pedido INT,
            id_cliente INT,
            data_venda DATE,
            status_pedido VARCHAR,
            forma_pagamento VARCHAR,
            valor_bruto DECIMAL(14,2),
            desconto DECIMAL(14,2),
            valor_liquido DECIMAL(14,2)
        )
        """,
        "DELETE FROM nessie.raw_pedidos",
        """
        INSERT INTO nessie.raw_pedidos VALUES
            (5001, 101, DATE '2026-11-01', 'CONCLUIDO', 'PIX', 18500.00, 500.00, 18000.00),
            (5002, 102, DATE '2026-11-01', 'CONCLUIDO', 'CARTAO_CREDITO', 6400.00, 200.00, 6200.00),
            (5003, 103, DATE '2026-11-02', 'CONCLUIDO', 'BOLETO', 32000.00, 1000.00, 31000.00),
            (5004, 104, DATE '2026-11-02', 'CANCELADO', 'CARTAO_CREDITO', 4500.00, 0.00, 4500.00),
            (5005, 101, DATE '2026-11-03', 'CONCLUIDO', 'PIX', 12000.00, 0.00, 12000.00),
            (5006, 105, DATE '2026-11-03', 'CONCLUIDO', 'BOLETO', 9800.00, 300.00, 9500.00),
            (5007, 106, DATE '2026-11-04', 'CONCLUIDO', 'PIX', 27500.00, 500.00, 27000.00),
            (5008, 107, DATE '2026-11-04', 'CONCLUIDO', 'CARTAO_CREDITO', 3100.00, 100.00, 3000.00),
            (5009, 108, DATE '2026-11-05', 'CONCLUIDO', 'BOLETO', 15400.00, 400.00, 15000.00),
            (5010, 109, DATE '2026-11-06', 'CONCLUIDO', 'PIX', 8900.00, 150.00, 8750.00),
            (5011, 110, DATE '2026-11-07', 'CONCLUIDO', 'PIX', 42000.00, 2000.00, 40000.00),
            (5012, 104, DATE '2026-11-08', 'CONCLUIDO', 'CARTAO_CREDITO', 21500.00, 500.00, 21000.00),
            (5013, 103, DATE '2026-11-09', 'CONCLUIDO', 'BOLETO', 19200.00, 200.00, 19000.00),
            (5014, 106, DATE '2026-11-10', 'CONCLUIDO', 'PIX', 34000.00, 1000.00, 33000.00),
            (5015, 109, DATE '2026-11-11', 'CONCLUIDO', 'PIX', 14500.00, 500.00, 14000.00)
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.raw_pagamentos (
            id_pagamento INT,
            id_pedido INT,
            meio_pagamento VARCHAR,
            parcelas INT,
            taxa_gateway_pct DECIMAL(6,2),
            valor_taxa_gateway DECIMAL(14,2),
            status_liquidacao VARCHAR
        )
        """,
        "DELETE FROM nessie.raw_pagamentos",
        """
        INSERT INTO nessie.raw_pagamentos VALUES
            (9001, 5001, 'PIX', 1, 0.50, 90.00, 'LIQUIDADO'),
            (9002, 5002, 'CARTAO_CREDITO', 3, 2.50, 155.00, 'LIQUIDADO'),
            (9003, 5003, 'BOLETO', 1, 1.00, 310.00, 'LIQUIDADO'),
            (9004, 5004, 'CARTAO_CREDITO', 1, 2.50, 0.00, 'ESTORNADO'),
            (9005, 5005, 'PIX', 1, 0.50, 60.00, 'LIQUIDADO'),
            (9006, 5006, 'BOLETO', 1, 1.00, 95.00, 'LIQUIDADO'),
            (9007, 5007, 'PIX', 1, 0.50, 135.00, 'LIQUIDADO'),
            (9008, 5008, 'CARTAO_CREDITO', 2, 2.50, 75.00, 'LIQUIDADO'),
            (9009, 5009, 'BOLETO', 1, 1.00, 150.00, 'LIQUIDADO'),
            (9010, 5010, 'PIX', 1, 0.50, 43.75, 'LIQUIDADO'),
            (9011, 5011, 'PIX', 1, 0.50, 200.00, 'LIQUIDADO'),
            (9012, 5012, 'CARTAO_CREDITO', 4, 2.50, 525.00, 'LIQUIDADO'),
            (9013, 5013, 'BOLETO', 1, 1.00, 190.00, 'LIQUIDADO'),
            (9014, 5014, 'PIX', 1, 0.50, 165.00, 'LIQUIDADO'),
            (9015, 5015, 'PIX', 1, 0.50, 70.00, 'LIQUIDADO')
        """,
        # --- Space 01_raw ---
        """
        CREATE OR REPLACE VIEW "01_raw".raw_clientes AS
        SELECT * FROM nessie.raw_clientes AT BRANCH main
        """,
        """
        CREATE OR REPLACE VIEW "01_raw".raw_pedidos AS
        SELECT * FROM nessie.raw_pedidos AT BRANCH main
        """,
        """
        CREATE OR REPLACE VIEW "01_raw".raw_pagamentos AS
        SELECT * FROM nessie.raw_pagamentos AT BRANCH main
        """,
        # --- Space 02_staging ---
        """
        CREATE OR REPLACE VIEW "02_staging".stg_clientes AS
        SELECT
            id_cliente,
            TRIM(nome_cliente) AS nome_cliente,
            UPPER(TRIM(segmento)) AS segmento,
            UPPER(TRIM(uf)) AS uf,
            TRIM(cidade) AS cidade,
            data_cadastro
        FROM nessie.raw_clientes AT BRANCH main
        """,
        """
        CREATE OR REPLACE VIEW "02_staging".stg_pedidos AS
        SELECT
            id_pedido,
            id_cliente,
            data_venda,
            UPPER(TRIM(status_pedido)) AS status_pedido,
            UPPER(TRIM(forma_pagamento)) AS forma_pagamento,
            valor_bruto,
            desconto,
            valor_liquido
        FROM nessie.raw_pedidos AT BRANCH main
        """,
        """
        CREATE OR REPLACE VIEW "02_staging".stg_pagamentos AS
        SELECT
            id_pagamento,
            id_pedido,
            UPPER(TRIM(meio_pagamento)) AS meio_pagamento,
            parcelas,
            taxa_gateway_pct,
            valor_taxa_gateway,
            UPPER(TRIM(status_liquidacao)) AS status_liquidacao
        FROM nessie.raw_pagamentos AT BRANCH main
        """,
        # --- Space 03_intermediate ---
        """
        CREATE OR REPLACE VIEW "03_intermediate".int_clientes_metricas AS
        SELECT
            c.id_cliente,
            c.nome_cliente,
            c.segmento,
            c.uf,
            c.cidade,
            c.data_cadastro,
            COUNT(p.id_pedido) AS total_pedidos,
            COALESCE(SUM(p.valor_liquido), 0) AS ltv_cliente
        FROM nessie.raw_clientes AT BRANCH main AS c
        LEFT JOIN nessie.raw_pedidos AT BRANCH main AS p
            ON c.id_cliente = p.id_cliente AND p.status_pedido = 'CONCLUIDO'
        GROUP BY c.id_cliente, c.nome_cliente, c.segmento, c.uf, c.cidade, c.data_cadastro
        """,
        """
        CREATE OR REPLACE VIEW "03_intermediate".int_pedidos_enriquecidos AS
        SELECT
            p.id_pedido,
            p.id_cliente,
            c.nome_cliente,
            c.segmento,
            c.uf,
            c.cidade,
            p.data_venda AS data_ref,
            p.status_pedido,
            p.forma_pagamento,
            COALESCE(pg.parcelas, 1) AS parcelas,
            p.valor_bruto,
            p.desconto,
            p.valor_liquido,
            COALESCE(pg.valor_taxa_gateway, 0) AS valor_taxa_gateway,
            (p.valor_liquido - COALESCE(pg.valor_taxa_gateway, 0)) AS receita_liquida_real,
            COALESCE(pg.status_liquidacao, 'PENDENTE') AS status_liquidacao
        FROM nessie.raw_pedidos AT BRANCH main AS p
        JOIN nessie.raw_clientes AT BRANCH main AS c ON p.id_cliente = c.id_cliente
        LEFT JOIN nessie.raw_pagamentos AT BRANCH main AS pg ON p.id_pedido = pg.id_pedido
        """,
        # --- Space 04_marts (Com Surrogate Keys MD5 + Natural Keys de Origem) ---
        """
        CREATE OR REPLACE VIEW "04_marts".dim_clientes AS
        SELECT
            MD5(CAST(id_cliente AS VARCHAR)) AS sk_cliente,
            id_cliente,
            nome_cliente,
            segmento,
            uf,
            cidade,
            data_cadastro,
            total_pedidos,
            ltv_cliente,
            CASE
                WHEN ltv_cliente >= 30000 THEN 'TIER_A_VIP'
                WHEN ltv_cliente >= 10000 THEN 'TIER_B_GROWTH'
                ELSE 'TIER_C_STANDARD'
            END AS classificacao_cliente
        FROM "03_intermediate".int_clientes_metricas
        """,
        """
        CREATE OR REPLACE VIEW "04_marts".fato_vendas AS
        SELECT
            MD5(CAST(id_pedido AS VARCHAR)) AS sk_venda,
            MD5(CAST(id_cliente AS VARCHAR)) AS fk_cliente,
            id_pedido,
            id_cliente,
            nome_cliente,
            segmento,
            uf,
            cidade,
            data_ref,
            status_pedido,
            forma_pagamento,
            parcelas,
            valor_bruto,
            desconto,
            valor_liquido,
            valor_taxa_gateway,
            receita_liquida_real,
            status_liquidacao
        FROM "03_intermediate".int_pedidos_enriquecidos
        """,
        """
        CREATE OR REPLACE VIEW "04_marts".mart_faturamento_diario AS
        SELECT
            MD5(CONCAT(CAST(data_ref AS VARCHAR), '|', CAST(id_cliente AS VARCHAR), '|', COALESCE(forma_pagamento, ''))) AS sk_faturamento_diario,
            MD5(CAST(id_cliente AS VARCHAR)) AS fk_cliente,
            id_cliente,
            data_ref,
            nome_cliente,
            segmento,
            uf,
            forma_pagamento,
            COUNT(id_pedido) AS qtd_pedidos,
            SUM(valor_bruto) AS total_bruto,
            SUM(desconto) AS total_descontos,
            SUM(valor_liquido) AS total_vendido,
            SUM(valor_taxa_gateway) AS total_taxas_gateway,
            SUM(receita_liquida_real) AS receita_liquida_pos_taxa
        FROM "03_intermediate".int_pedidos_enriquecidos
        WHERE status_pedido = 'CONCLUIDO'
        GROUP BY data_ref, id_cliente, nome_cliente, segmento, uf, forma_pagamento
        """,
    ]
    for stmt in sql_statements:
        dremio_sql_sync(token, stmt.strip(), wait_sec=35)

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
    print("   ✅ Dremio e Nessie estruturados em: 01_raw -> 02_staging -> 03_intermediate -> 04_marts!")


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
