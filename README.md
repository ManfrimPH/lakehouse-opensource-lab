# 🚀 Lakehouse Open Source Lab — Do Dado ao Dashboard com Open Source

Laboratório prático completo executando localmente em **Docker Compose** toda a arquitetura de **6 camadas** apresentada na palestra **"Do Dado ao Dashboard com Open Source"** (*Pedro Henrique Santos Manfrim*).

---

## 🏗️ Arquitetura e Portas dos Serviços Locais

| Camada | Ferramenta | Função na Arquitetura | URL / Porta Local | Credenciais |
| :--- | :--- | :--- | :--- | :--- |
| **0. Object Storage** | **MinIO (S3)** | Bucket S3 local (`s3://lakehouse`) onde os arquivos `.parquet` e metadados Iceberg residem | Console: `http://localhost:9101` <br> API S3: `http://localhost:9100` | `minioadmin` / `minioadmin123` |
| **1 & 2. Tabelas ACID & Git** | **Apache Iceberg + Project Nessie** | Catálogo transacional com semântica Git (`main`, `etl_diario`, commits e merges atômicos) | UI & API: `http://localhost:19120` | *(Acesso direto)* |
| **3. Transformação & Testes** | **dbt-core** | Engenharia de software no ELT: modelos SQL declarativos (`{{ ref }}`), linhagem e testes | Container `lakehouse_airflow` (`/opt/airflow/dbt_lakehouse`) | — |
| **4. Motor & Semântica** | **Dremio (Apache Arrow)** | Engine colunar vetorizada em memória, Camada Semântica (`raw`, `marts`) e Catálogo Nessie | Web UI: `http://localhost:9047` <br> Arrow Flight: `localhost:32010` | `admin` / `Admin1234!` |
| **5. Visualização & Cache** | **Apache Superset + Redis** | Self-Service BI sem custo por assento + SQL Lab IDE + Cache Redis sincronizado | Web UI: `http://localhost:8088` <br> Redis: `localhost:6380` | `admin` / `admin` |
| **6. Orquestração (Maestro)** | **Apache Airflow** | Motor de **Data CI/CD**: cria branch no Nessie, roda dbt, valida testes, faz merge e limpa cache | Web UI: `http://localhost:8085` | `admin` / `admin` |

---

## ⚡ Como Subir ou Gerenciar o Laboratório

```bash
# 0. Configurar variáveis de ambiente a partir do template de produção:
cp .env.example .env

# 1. Subir todos os containers e executar o bootstrap automático de todas as camadas:
make up

# 2. Verificar o status de saúde de todos os serviços:
make status

# 3. Rodar os testes de integridade da Alfândega de Dados (dbt test):
make dbt-test

# 4. Disparar o Pipeline End-to-End de Data CI/CD (7 Passos do Slide 20):
make demo-cicd
```

---

## 🎤 Roteiro de Demonstração Ao Vivo (Para o Palco)

### 1. Mostrar o Git para Dados (Project Nessie + Apache Iceberg no MinIO)
- Abra o **Project Nessie** em `http://localhost:19120` para mostrar as branches `main`, `etl_diario` e `homologacao`.
- Abra o **MinIO** em `http://localhost:9101` (bucket `lakehouse`) para mostrar os arquivos físicos `.parquet` e a árvore de metadados (`metadata/*.json` e `.avro`) do **Apache Iceberg**.

### 2. Mostrar o Motor Vetorizado e o Catálogo Unificado no Dremio
- Abra o **Dremio** em `http://localhost:9047` (`admin` / `Admin1234!`).
- Mostre a fonte oficial **`nessie`** conectada com seletor de branches (`main` / `etl_diario`) e os Spaces amigáveis **`raw`** (dados brutos de auditoria) e **`marts`** (fatos, dimensões e agregações de negócio com chaves MD5) para navegação visual e consumo.
- Execute consultas no SQL Lab com velocidade em milissegundos via Apache Arrow.

### 3. Mostrar o Data CI/CD e o Circuit Breaker no Apache Airflow
- Abra o **Apache Airflow** em `http://localhost:8085` (`admin` / `admin`).
- **DAG `lakehouse_data_cicd_pipeline`:** Executa a esteira completa com **Circuit Breaker** nativo (Criação de Branch isolada no Nessie ➔ Landing Parquet no MinIO S3 ➔ Raw Ingestion ➔ Staging 1:1 ➔ Intermediate com MD5 e métricas ➔ Marts dimensionais e fatos com testes em paralelo ➔ Merge Atômico na `main` somente se 100% dos testes passarem ➔ Invalidação cirúrgica de cache Redis do Superset).
- Caso qualquer teste falhe na **Alfândega de Dados**, o pipeline desarma o disjuntor (Circuit Breaker), bloqueia o merge atômico e protege os dashboards de produção intactos!

### 4. Mostrar o Consumo Democrático no Apache Superset
- Abra o **Apache Superset** em `http://localhost:8088` (`admin` / `admin`).
- Em **Datasets**, explore `faturamento_diario`, `fato_vendas` e `dim_clientes`, ou abra o **SQL Lab** para consultar o Lakehouse em milissegundos com cache Redis ativo.
