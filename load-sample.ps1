Write-Host ""
Write-Host "📤 Loading sample agent card into MinIO..."
Write-Host ""

# Check processor is running
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing -ErrorAction Stop
} catch {
    Write-Host "❌ Processor service is not running."
    Write-Host "   Please run quickstart first:"
    Write-Host "   iwr -useb https://raw.githubusercontent.com/rahul-tavro/agent-catalog-docker/main/quickstart.ps1 | iex"
    exit 1
}

# Upload sample JSON to MinIO
$command = "mc alias set local http://minio:9000 minioadmin minioadmin && mc cp /sample-data/agent_card_sample.json local/warehouse/raw/agent_card_json/agent_card_sample.json && echo 'Sample file uploaded'"

docker run --rm `
    --network agent_catalog_docker_default `
    -v "${PWD}/sample-data:/sample-data:ro" `
    minio/mc:latest `
    /bin/sh -c $command

Write-Host ""
Write-Host "⚡ MinIO event fired — pipeline is running..."
Write-Host ""
Write-Host "📋 Watch pipeline logs:"
Write-Host "   docker logs -f agent_catalog_processor"
Write-Host ""
Write-Host "🔍 Verify data in MinIO:"
Write-Host "   http://localhost:9001  (minioadmin / minioadmin)"