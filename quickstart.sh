#!/bin/bash
set -e

echo ""
echo "=================================="
echo "   Agent Catalog Quickstart"
echo "=================================="
echo ""

# Check docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker Desktop first:"
    echo "   https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi

# Check docker compose is available
if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose not found. Please update Docker Desktop."
    exit 1
fi

echo "✅ Docker is ready"
echo ""
echo "📦 Downloading docker-compose.yml..."
curl -sSL https://raw.githubusercontent.com/rahul-tavro/agent-catalog-docker/main/docker-compose.yml -o docker-compose.yml

echo "🚀 Starting Agent Catalog..."
docker compose up -d

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 10

# Wait for ddl-init to complete
echo "📋 Creating Iceberg tables..."
while [ "$(docker inspect -f '{{.State.Status}}' agent_catalog_ddl_init 2>/dev/null)" != "exited" ]; do
    echo "   Still creating tables..."
    sleep 5
done

EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' agent_catalog_ddl_init)

if [ "$EXIT_CODE" = "0" ]; then
    echo ""
    echo "✅ Agent Catalog is ready!"
    echo ""
    echo "📌 Services:"
    echo "   MinIO Console  → http://localhost:9001  (minioadmin / minioadmin)"
    echo "   Nessie API     → http://localhost:19120/api/v1/config"
    echo ""
    echo "📊 Tables created in namespaces:"
    echo "   - nessie.catalog_core"
    echo "   - nessie.catalog_curated"
    echo "   - nessie.catalog_raw"
    echo ""
    echo "To stop:    docker compose down"
    echo "To reset:   docker compose down -v"
else
    echo ""
    echo "❌ Table creation failed. Check logs:"
    echo "   docker logs agent_catalog_ddl_init"
fi
