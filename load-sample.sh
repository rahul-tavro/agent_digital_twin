#!/bin/bash
set -e

echo ""
echo "📤 Loading sample agent card into MinIO..."
echo ""

# Check processor is running
if ! curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo "❌ Processor service is not running."
    echo "   Please run quickstart first:"
    echo "   curl -sSL https://raw.githubusercontent.com/rahul-tavro/agent-catalog-docker/main/quickstart.sh | bash"
    exit 1
fi

# Download sample JSON and upload to MinIO via mc
docker run --rm \
    --network agent_catalog_docker_default \
    -v "$(pwd)/sample-data:/sample-data:ro" \
    minio/mc:latest \
    /bin/sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc cp /sample-data/agent_card_sample.json local/warehouse/raw/agent_card_json/agent_card_sample.json &&
        echo '✅ Sample file uploaded to MinIO'
    "

echo ""
echo "⚡ MinIO event fired — pipeline is running..."
echo ""
echo "📋 Watch pipeline logs:"
echo "   docker logs -f agent_catalog_processor"
echo ""
echo "🔍 Verify data in MinIO:"
echo "   http://localhost:9001  (minioadmin / minioadmin)"