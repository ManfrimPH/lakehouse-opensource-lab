#!/bin/bash
set -e

echo "=== [Superset Init] Instalando drivers (psycopg2-binary, redis) ==="
pip install --no-cache-dir --no-deps psycopg2-binary redis >/dev/null 2>&1 || true

echo "=== [Superset Init] Aplicando migrações do banco de metadados ==="
superset db upgrade

echo "=== [Superset Init] Criando usuário administrador (admin / admin) ==="
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname "Pedro" \
    --lastname "Manfrim" \
    --email "pedro@lakehouse.local" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

echo "=== [Superset Init] Inicializando permissões e papéis RBAC ==="
superset init

echo "=== [Superset Init] Iniciando servidor web na porta 8088 ==="
exec /usr/bin/run-server.sh
