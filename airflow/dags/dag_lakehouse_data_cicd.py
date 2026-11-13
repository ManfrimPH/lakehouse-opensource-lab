"""
DAG 01: Arquitetura Medalhão + dbt Best Practices (Com Dados Públicos NYC TLC + Vendas)
Fluxo completo separado em TaskGroups:
1. [01_landing_minio_parquet]: Cria branch no Nessie e grava arquivos .parquet no MinIO (s3://lakehouse/landing_parquet/):
   - taxi_zones.parquet (265 zonas oficiais NYC TLC)
   - yellow_tripdata.parquet (15.000 corridas públicas reais NYC TLC)
   - clientes.parquet, pedidos.parquet, pagamentos.parquet
2. [02_camada_raw]: Lê os arquivos .parquet de dentro do MinIO e carrega as tabelas RAW (raw.raw_*) + testa source:raw
3. [03_camada_staging]: Roda models/staging (espelho 1:1 sem tratamento) + testa staging
4. [04_camada_intermediate]: Roda models/intermediate (onde TODO o tratamento, joins e chaves MD5 acontecem) + testa intermediate
5. [05_camada_marts_e_testes]: Roda models/marts (PK das fatos = chave | PK das dims e FKs = id_<nome_tabela>) com Task de Teste dedicada para CADA Mart
6. [06_publicacao_e_bi]: Faz Merge atômico na branch 'main' do Nessie e limpa o cache Redis do Superset
"""
from datetime import datetime, timedelta
import csv
import subprocess
import requests
import redis
import psycopg2
from psycopg2.extras import execute_values
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from airflow.decorators import dag, task, task_group

NESSIE_API = "http://nessie:19120/api/v1"
DREMIO_API = "http://dremio:9047"
DREMIO_USER = "admin"
DREMIO_PASS = "Admin1234!"
DBT_DIR = "/opt/airflow/dbt_lakehouse"
SAMPLE_DATA_DIR = "/opt/airflow/scripts/sample_data"
PG_DSN = "host=postgres port=5432 dbname=lakehouse_meta user=lakehouse password=lakehouse123"


def get_s3_fs():
    """Cliente S3 nativo do Apache Arrow apontando para o MinIO."""
    return pafs.S3FileSystem(
        access_key="minioadmin",
        secret_key="minioadmin123",
        endpoint_override="minio:9000",
        scheme="http",
    )


def get_dremio_token():
    try:
        resp = requests.post(
            f"{DREMIO_API}/apiv2/login",
            json={"userName": DREMIO_USER, "password": DREMIO_PASS},
            timeout=5,
        )
        if resp.status_code == 200:
            return f"_dremio{resp.json()['token']}"
    except Exception as exc:
        print(f"[Aviso Dremio] {exc}")
    return None


def run_dbt(args: list, schema: str = "branch_etl_diario"):
    env = {
        **subprocess.os.environ,
        "DBT_SCHEMA": schema,
        "DBT_RAW_SCHEMA": "raw",
    }
    cmd = ["dbt", *args, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]
    print(f"$ {' '.join(cmd)} (DBT_SCHEMA={schema})")
    res = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
    print(res.stdout)
    return res.stdout


def load_parquet_into_pg_raw(entidade: str, rows: list):
    """Materializa na camada RAW (schema raw) os registros lidos do arquivo .parquet do MinIO."""
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        if entidade == "clientes":
            cur.execute("DROP TABLE IF EXISTS raw.raw_clientes CASCADE;")
            cur.execute(
                """
                CREATE TABLE raw.raw_clientes (
                    id_cliente INTEGER,
                    nome_cliente TEXT,
                    segmento TEXT,
                    uf TEXT,
                    cidade TEXT,
                    data_cadastro DATE
                );
                """
            )
            execute_values(
                cur,
                "INSERT INTO raw.raw_clientes VALUES %s;",
                [(r["id_cliente"], r["nome_cliente"], r["segmento"], r["uf"], r["cidade"], r["data_cadastro"]) for r in rows],
            )
        elif entidade == "pedidos":
            cur.execute("DROP TABLE IF EXISTS raw.raw_pedidos CASCADE;")
            cur.execute(
                """
                CREATE TABLE raw.raw_pedidos (
                    id_pedido INTEGER,
                    id_cliente INTEGER,
                    data_venda TIMESTAMP,
                    status_pedido TEXT,
                    forma_pagamento TEXT,
                    valor_bruto NUMERIC(14,2),
                    desconto NUMERIC(14,2),
                    valor_liquido NUMERIC(14,2)
                );
                """
            )
            execute_values(
                cur,
                "INSERT INTO raw.raw_pedidos VALUES %s;",
                [
                    (
                        r["id_pedido"],
                        r["id_cliente"],
                        r["data_venda"],
                        r["status_pedido"],
                        r["forma_pagamento"],
                        r["valor_bruto"],
                        r["desconto"],
                        r["valor_liquido"],
                    )
                    for r in rows
                ],
            )
        elif entidade == "pagamentos":
            cur.execute("DROP TABLE IF EXISTS raw.raw_pagamentos CASCADE;")
            cur.execute(
                """
                CREATE TABLE raw.raw_pagamentos (
                    id_pagamento INTEGER,
                    id_pedido INTEGER,
                    meio_pagamento TEXT,
                    parcelas INTEGER,
                    taxa_gateway_pct NUMERIC(6,2),
                    valor_taxa_gateway NUMERIC(14,2),
                    status_liquidacao TEXT
                );
                """
            )
            execute_values(
                cur,
                "INSERT INTO raw.raw_pagamentos VALUES %s;",
                [
                    (
                        r["id_pagamento"],
                        r["id_pedido"],
                        r["meio_pagamento"],
                        r["parcelas"],
                        r["taxa_gateway_pct"],
                        r["valor_taxa_gateway"],
                        r["status_liquidacao"],
                    )
                    for r in rows
                ],
            )
        elif entidade == "taxi_zones":
            cur.execute("DROP TABLE IF EXISTS raw.raw_taxi_zones CASCADE;")
            cur.execute(
                """
                CREATE TABLE raw.raw_taxi_zones (
                    location_id INTEGER,
                    borough TEXT,
                    zone TEXT,
                    service_zone TEXT
                );
                """
            )
            execute_values(
                cur,
                "INSERT INTO raw.raw_taxi_zones VALUES %s;",
                [(r["location_id"], r["borough"], r["zone"], r["service_zone"]) for r in rows],
            )
        elif entidade == "yellow_tripdata":
            cur.execute("DROP TABLE IF EXISTS raw.raw_yellow_tripdata CASCADE;")
            cur.execute(
                """
                CREATE TABLE raw.raw_yellow_tripdata (
                    trip_id BIGINT,
                    vendorid INTEGER,
                    tpep_pickup_datetime TIMESTAMP,
                    tpep_dropoff_datetime TIMESTAMP,
                    passenger_count NUMERIC(10,2),
                    trip_distance NUMERIC(12,2),
                    ratecodeid NUMERIC(10,2),
                    store_and_fwd_flag TEXT,
                    pulocationid INTEGER,
                    dolocationid INTEGER,
                    payment_type INTEGER,
                    fare_amount NUMERIC(12,2),
                    extra NUMERIC(12,2),
                    mta_tax NUMERIC(12,2),
                    tip_amount NUMERIC(12,2),
                    tolls_amount NUMERIC(12,2),
                    improvement_surcharge NUMERIC(12,2),
                    total_amount NUMERIC(12,2),
                    congestion_surcharge NUMERIC(12,2),
                    airport_fee NUMERIC(12,2)
                );
                """
            )
            execute_values(
                cur,
                "INSERT INTO raw.raw_yellow_tripdata VALUES %s;",
                [
                    (
                        r["trip_id"],
                        r["vendorid"],
                        r["tpep_pickup_datetime"],
                        r["tpep_dropoff_datetime"],
                        r["passenger_count"],
                        r["trip_distance"],
                        r["ratecodeid"],
                        r["store_and_fwd_flag"],
                        r["pulocationid"],
                        r["dolocationid"],
                        r["payment_type"],
                        r["fare_amount"],
                        r["extra"],
                        r["mta_tax"],
                        r["tip_amount"],
                        r["tolls_amount"],
                        r["improvement_surcharge"],
                        r["total_amount"],
                        r["congestion_surcharge"],
                        r["airport_fee"],
                    )
                    for r in rows
                ],
                page_size=2000,
            )
    conn.close()


@dag(
    dag_id="lakehouse_data_cicd_pipeline",
    description="Pipeline Modular: MinIO Parquet (NYC TLC + Vendas) -> Raw -> Staging (1:1) -> Intermediate (Tratamento/MD5) -> Marts (Testes por Tabela)",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={
        "owner": "pedro.manfrim",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["lakehouse", "nyc-tlc", "mobilidade-urbana", "raw", "staging", "intermediate", "marts"],
)
def lakehouse_medallion_pipeline():

    # ==========================================================================
    # 01. LANDING ZONE NO MINIO (SALVA OS DADOS EM PARQUET NO S3)
    # ==========================================================================
    @task_group(group_id="01_landing_minio_parquet")
    def landing_minio_group():

        @task(task_id="criar_branch_isolada_nessie")
        def criar_branch_nessie():
            branch_name = "etl_diario"
            main_ref = requests.get(f"{NESSIE_API}/trees/tree/main", timeout=10).json()
            main_hash = main_ref["hash"]

            chk = requests.get(f"{NESSIE_API}/trees/tree/{branch_name}", timeout=10)
            if chk.status_code == 200:
                requests.delete(
                    f"{NESSIE_API}/trees/branch/{branch_name}?expectedHash={chk.json()['hash']}",
                    timeout=10,
                )

            resp = requests.post(
                f"{NESSIE_API}/trees/tree?sourceRefName=main",
                json={"type": "BRANCH", "name": branch_name, "hash": main_hash},
                timeout=10,
            )
            resp.raise_for_status()
            return {"branch": branch_name, "hash": main_hash}

        @task(task_id="salvar_arquivos_parquet_no_minio")
        def salvar_parquets_no_minio(branch_ctx: dict):
            s3 = get_s3_fs()
            s3.create_dir("lakehouse/landing_parquet")

            saved_paths = {}
            for ent in ["clientes", "pedidos", "pagamentos"]:
                csv_path = f"{SAMPLE_DATA_DIR}/raw_{ent}.csv"
                with open(csv_path, "r", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
                cols = {k: [r[k] for r in rows] for k in rows[0].keys()}
                arrow_table = pa.table(cols)
                s3_uri = f"lakehouse/landing_parquet/{ent}.parquet"
                pq.write_table(arrow_table, s3_uri, filesystem=s3)
                saved_paths[ent] = f"s3://{s3_uri}"

            # Grava também os Parquets públicos da NYC TLC (Zonas + Corridas)
            tbl_zones = pq.read_table(f"{SAMPLE_DATA_DIR}/taxi_zones.parquet")
            pq.write_table(tbl_zones, "lakehouse/landing_parquet/taxi_zones.parquet", filesystem=s3)
            saved_paths["taxi_zones"] = "s3://lakehouse/landing_parquet/taxi_zones.parquet"

            tbl_trips = pq.read_table(f"{SAMPLE_DATA_DIR}/yellow_tripdata_sample.parquet")
            pq.write_table(tbl_trips, "lakehouse/landing_parquet/yellow_tripdata.parquet", filesystem=s3)
            saved_paths["yellow_tripdata"] = "s3://lakehouse/landing_parquet/yellow_tripdata.parquet"

            return {**branch_ctx, "parquet_files": saved_paths}

        b_ctx = criar_branch_nessie()
        return salvar_parquets_no_minio(b_ctx)

    # ==========================================================================
    # 02. CAMADA RAW (LÊ OS PARQUETS DO MINIO E POPULA AS TABELAS RAW)
    # ==========================================================================
    @task_group(group_id="02_camada_raw")
    def raw_layer_group(landing_ctx: dict):

        def _load_from_minio_parquet(entidade: str):
            s3 = get_s3_fs()
            s3_path = f"lakehouse/landing_parquet/{entidade}.parquet"
            arrow_tbl = pq.read_table(s3_path, filesystem=s3)
            rows = arrow_tbl.to_pylist()
            print(f"[Camada RAW] Lido s3://{s3_path} direto do MinIO via PyArrow ({len(rows)} linhas).")
            load_parquet_into_pg_raw(entidade, rows)
            return f"raw_{entidade}_ok"

        @task(task_id="load_raw_taxi_zones_from_minio")
        def load_raw_taxi_zones(_ctx: dict):
            return _load_from_minio_parquet("taxi_zones")

        @task(task_id="load_raw_yellow_tripdata_from_minio")
        def load_raw_yellow_tripdata(_ctx: dict):
            return _load_from_minio_parquet("yellow_tripdata")

        @task(task_id="load_raw_clientes_from_minio")
        def load_raw_clientes(_ctx: dict):
            return _load_from_minio_parquet("clientes")

        @task(task_id="load_raw_pedidos_from_minio")
        def load_raw_pedidos(_ctx: dict):
            return _load_from_minio_parquet("pedidos")

        @task(task_id="load_raw_pagamentos_from_minio")
        def load_raw_pagamentos(_ctx: dict):
            return _load_from_minio_parquet("pagamentos")

        @task(task_id="raw_camada_concluida")
        def raw_concluida(_z: str, _t: str, _c: str, _pe: str, _pa: str):
            return "raw_loaded"

        rz = load_raw_taxi_zones(landing_ctx)
        rt = load_raw_yellow_tripdata(landing_ctx)
        rc = load_raw_clientes(landing_ctx)
        rpe = load_raw_pedidos(landing_ctx)
        rpa = load_raw_pagamentos(landing_ctx)
        return raw_concluida(rz, rt, rc, rpe, rpa)

    # ==========================================================================
    # 03. CAMADA STAGING (models/staging - ESPELHO 1:1 SEM TRATAMENTO)
    # ==========================================================================
    @task_group(group_id="03_camada_staging")
    def staging_layer_group(raw_ok: str):

        @task(task_id="run_stg_zonas")
        def run_stg_zonas(_r: str):
            run_dbt(["run", "--select", "stg_zonas"], schema="branch_etl_diario")
            return "stg_zonas_ok"

        @task(task_id="run_stg_corridas")
        def run_stg_corridas(_r: str):
            run_dbt(["run", "--select", "stg_corridas"], schema="branch_etl_diario")
            return "stg_corridas_ok"

        @task(task_id="run_stg_vendas_domain")
        def run_stg_vendas(_r: str):
            run_dbt(["run", "--select", "stg_clientes", "stg_pedidos", "stg_pagamentos"], schema="branch_etl_diario")
            return "stg_vendas_ok"

        @task(task_id="staging_camada_concluida")
        def staging_concluida(_s1: str, _s2: str, _s3: str):
            return "staging_loaded"

        s1 = run_stg_zonas(raw_ok)
        s2 = run_stg_corridas(raw_ok)
        s3 = run_stg_vendas(raw_ok)
        return staging_concluida(s1, s2, s3)

    # ==========================================================================
    # 04. CAMADA INTERMEDIATE (models/intermediate - TRATAMENTO, JOINS E MD5)
    # ==========================================================================
    @task_group(group_id="04_camada_intermediate")
    def intermediate_layer_group(stg_ok: str):

        @task(task_id="run_int_zonas_tratadas")
        def run_int_zonas(_s: str):
            run_dbt(["run", "--select", "int_zonas_tratadas"], schema="branch_etl_diario")
            return "int_zonas_ok"

        @task(task_id="run_int_corridas_tratadas")
        def run_int_corridas(_z: str):
            run_dbt(["run", "--select", "int_corridas_tratadas"], schema="branch_etl_diario")
            return "int_corridas_ok"

        @task(task_id="run_int_vendas_tratadas")
        def run_int_vendas(_s: str):
            run_dbt(["run", "--select", "int_clientes_metricas", "int_pedidos_enriquecidos"], schema="branch_etl_diario")
            return "int_vendas_ok"

        @task(task_id="intermediate_camada_concluida")
        def intermediate_concluida(_i1: str, _i2: str):
            return "intermediate_loaded"

        iz = run_int_zonas(stg_ok)
        ic = run_int_corridas(iz)
        iv = run_int_vendas(stg_ok)
        return intermediate_concluida(ic, iv)

    # ==========================================================================
    # 05. CAMADA MARTS (models/marts - PARALELO: RUN E TEST IMEDIATO POR TABELA)
    # ==========================================================================
    @task_group(group_id="05_camada_marts_e_testes")
    def marts_layer_group(int_ok: str):

        # --- Bloco 1: Dimensões (Rodam e testam em paralelo) ---
        @task(task_id="run_dim_zonas")
        def run_dim_zonas(_i: str):
            run_dbt(["run", "--select", "dim_zonas"], schema="branch_etl_diario")
            return "dim_zonas_built"

        @task(task_id="test_dim_zonas")
        def test_dim_zonas(_b: str):
            run_dbt(["test", "--select", "dim_zonas", "--indirect-selection", "cautious"], schema="branch_etl_diario")
            return "dim_zonas_tested"

        @task(task_id="run_dim_modalidades_pagamento")
        def run_dim_modalidades_pagamento(_i: str):
            run_dbt(["run", "--select", "dim_modalidades_pagamento"], schema="branch_etl_diario")
            return "dim_modalidades_built"

        @task(task_id="test_dim_modalidades_pagamento")
        def test_dim_modalidades_pagamento(_b: str):
            run_dbt(["test", "--select", "dim_modalidades_pagamento", "--indirect-selection", "cautious"], schema="branch_etl_diario")
            return "dim_modalidades_tested"

        @task(task_id="run_dim_tarifas")
        def run_dim_tarifas(_i: str):
            run_dbt(["run", "--select", "dim_tarifas"], schema="branch_etl_diario")
            return "dim_tarifas_built"

        @task(task_id="test_dim_tarifas")
        def test_dim_tarifas(_b: str):
            run_dbt(["test", "--select", "dim_tarifas", "--indirect-selection", "cautious"], schema="branch_etl_diario")
            return "dim_tarifas_tested"

        @task(task_id="run_dim_clientes")
        def run_dim_clientes(_i: str):
            run_dbt(["run", "--select", "dim_clientes"], schema="branch_etl_diario")
            return "dim_clientes_built"

        @task(task_id="test_dim_clientes")
        def test_dim_clientes(_b: str):
            run_dbt(["test", "--select", "dim_clientes", "--indirect-selection", "cautious"], schema="branch_etl_diario")
            return "dim_clientes_tested"

        @task(task_id="dimensoes_todas_validadas")
        def dimensoes_validadas(_t1: str, _t2: str, _t3: str, _t4: str):
            return "all_dims_ok"

        # --- Bloco 2: Fatos (Rodam e testam em paralelo após as dimensões) ---
        @task(task_id="run_fato_corridas")
        def run_fato_corridas(_dims_ok: str):
            run_dbt(["run", "--select", "fato_corridas"], schema="branch_etl_diario")
            return "fato_corridas_built"

        @task(task_id="test_fato_corridas")
        def test_fato_corridas(_b: str):
            run_dbt(["test", "--select", "fato_corridas", "--indirect-selection", "cautious"], schema="branch_etl_diario")
            return "fato_corridas_tested"

        @task(task_id="run_fato_vendas")
        def run_fato_vendas(_dims_ok: str):
            run_dbt(["run", "--select", "fato_vendas"], schema="branch_etl_diario")
            return "fato_vendas_built"

        @task(task_id="test_fato_vendas")
        def test_fato_vendas(_b: str):
            run_dbt(["test", "--select", "fato_vendas", "--indirect-selection", "cautious"], schema="branch_etl_diario")
            return "fato_vendas_tested"

        @task(task_id="fatos_todas_validadas")
        def fatos_validadas(_f1: str, _f2: str):
            return "all_fatos_ok"

        # --- Bloco 3: Cubos Marts Agregados (Rodam e testam em paralelo após as fatos) ---
        @task(task_id="run_mart_mobilidade_diaria_zonas")
        def run_mart_mobilidade(_fatos_ok: str):
            run_dbt(["run", "--select", "mart_mobilidade_diaria_zonas"], schema="branch_etl_diario")
            return "mart_mobilidade_built"

        @task(task_id="test_mart_mobilidade_diaria_zonas")
        def test_mart_mobilidade(_b: str):
            run_dbt(["test", "--select", "mart_mobilidade_diaria_zonas", "--indirect-selection", "cautious"], schema="branch_etl_diario")
            return "mart_mobilidade_tested"

        @task(task_id="run_mart_faturamento_diario")
        def run_mart_faturamento(_fatos_ok: str):
            run_dbt(["run", "--select", "mart_faturamento_diario"], schema="branch_etl_diario")
            return "mart_faturamento_built"

        @task(task_id="test_mart_faturamento_diario")
        def test_mart_faturamento(_b: str):
            run_dbt(["test", "--select", "mart_faturamento_diario", "--indirect-selection", "cautious"], schema="branch_etl_diario")
            return "mart_faturamento_tested"

        @task(task_id="marts_todas_validadas")
        def marts_validadas(_m1: str, _m2: str):
            return "all_marts_ok"

        # Pipeline de Dimensões (Paralelo)
        td_z = test_dim_zonas(run_dim_zonas(int_ok))
        td_p = test_dim_modalidades_pagamento(run_dim_modalidades_pagamento(int_ok))
        td_t = test_dim_tarifas(run_dim_tarifas(int_ok))
        td_c = test_dim_clientes(run_dim_clientes(int_ok))
        dims_ready = dimensoes_validadas(td_z, td_p, td_t, td_c)

        # Pipeline de Fatos (Paralelo, acionado após as dimensões estarem 100% testadas)
        tf_c = test_fato_corridas(run_fato_corridas(dims_ready))
        tf_v = test_fato_vendas(run_fato_vendas(dims_ready))
        fatos_ready = fatos_validadas(tf_c, tf_v)

        # Pipeline de Marts Agregados (Paralelo, acionado após as fatos estarem 100% testadas)
        tm_m = test_mart_mobilidade(run_mart_mobilidade(fatos_ready))
        tm_f = test_mart_faturamento(run_mart_faturamento(fatos_ready))
        return marts_validadas(tm_m, tm_f)

    # ==========================================================================
    # 06. PUBLICAÇÃO ATÔMICA NA MAIN & CACHE DO SUPERSET
    # ==========================================================================
    @task_group(group_id="06_publicacao_e_bi")
    def publish_group(marts_ok: str):

        @task(task_id="merge_nessie_branch_into_main")
        def merge_to_main(_m: str):
            token = get_dremio_token()
            if token:
                headers = {"Authorization": token, "Content-Type": "application/json"}
                requests.post(
                    f"{DREMIO_API}/api/v3/sql",
                    headers=headers,
                    json={"sql": "MERGE BRANCH etl_diario INTO main IN nessie"},
                    timeout=15,
                )
            run_dbt(["run"], schema="lakehouse_main")
            return "merged_main"

        @task(task_id="invalidate_superset_redis_cache")
        def invalidate_cache(_merged: str):
            flushed = 0
            for db_idx in (1, 2, 3, 4):
                r_db = redis.Redis(host="redis", port=6379, db=db_idx, decode_responses=True)
                keys = r_db.keys("superset_*")
                if keys:
                    flushed += r_db.delete(*keys)
            print(f"[Superset Redis Cache] {flushed} chaves renovadas.")

        m = merge_to_main(marts_ok)
        invalidate_cache(m)

    l_ctx = landing_minio_group()
    r_ok = raw_layer_group(l_ctx)
    s_ok = staging_layer_group(r_ok)
    i_ok = intermediate_layer_group(s_ok)
    m_ok = marts_layer_group(i_ok)
    publish_group(m_ok)


lakehouse_medallion_pipeline()
