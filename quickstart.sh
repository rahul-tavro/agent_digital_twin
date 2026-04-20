#!/bin/bash
set -e

echo ""
echo "=================================="
echo "   AGENT CATALOG - QUICKSTART"
echo "=================================="
echo ""

# Check docker is installed
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker is not installed. Please install Docker Desktop first:"
    echo "        https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check docker is running
if ! docker info &> /dev/null; then
    echo "[ERROR] Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi

# Check docker compose is available
if ! docker compose version &> /dev/null; then
    echo "[ERROR] Docker Compose not found. Please update Docker Desktop."
    exit 1
fi

echo "[OK] Docker is ready"
echo ""
echo "[INFO] Downloading docker-compose.yml..."
curl -sSL https://raw.githubusercontent.com/rahul-tavro/agent-catalog-docker/main/docker-compose.yml -o docker-compose.yml

echo "[INFO] Starting Agent Catalog..."
docker compose up -d

echo ""
echo "[INFO] Waiting for services to be ready..."
sleep 10

echo "[INFO] Creating Iceberg tables..."
while [ "$(docker inspect -f '{{.State.Status}}' agent_catalog_ddl_init 2>/dev/null)" != "exited" ]; do
    echo "       Still creating tables..."
    sleep 5
done

EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' agent_catalog_ddl_init)

if [ "$EXIT_CODE" = "0" ]; then
    echo ""
    echo "=================================="
    echo "   AGENT CATALOG IS READY!"
    echo "=================================="
    echo ""
    echo "Services:"
    echo "   MinIO Console  -> http://localhost:9001  (minioadmin / minioadmin)"
    echo "   Nessie API     -> http://localhost:19120/api/v1/config"
    echo "   Processor      -> http://localhost:8000/health"
    echo ""
    echo "Tables created in namespaces:"
    echo "   - nessie.catalog_core"
    echo "   - nessie.catalog_curated"
    echo "   - nessie.catalog_raw"
    echo "   - nessie.catalog_risk_management"
    echo ""
    echo "To stop:    docker compose down"
    echo "To reset:   docker compose down -v"
else
    echo ""
    echo "[ERROR] Table creation failed. Check logs:"
    echo "        docker logs agent_catalog_ddl_init"
fi
