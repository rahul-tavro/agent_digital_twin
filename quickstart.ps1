Write-Host ""
Write-Host "=================================="
Write-Host "   AGENT CATALOG - QUICKSTART"
Write-Host "=================================="
Write-Host ""

# Check docker is installed
if (!(Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Docker is not installed. Please install Docker Desktop:"
    Write-Host "   https://www.docker.com/products/docker-desktop"
    exit 1
}

# Check docker is running
docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Docker is not running. Please start Docker Desktop and try again."
    exit 1
}

Write-Host "✅ Docker is ready"
Write-Host ""
Write-Host "📦 Downloading docker-compose.yml..."
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/rahul-tavro/agent-catalog-docker/main/docker-compose.yml" -OutFile "docker-compose.yml"

Write-Host "🚀 Starting Agent Catalog..."
docker compose up -d

Write-Host ""
Write-Host "⏳ Waiting for services to be ready..."
Start-Sleep -Seconds 10

Write-Host "📋 Creating Iceberg tables..."
while ((docker inspect -f '{{.State.Status}}' agent_catalog_ddl_init 2>$null) -ne "exited") {
    Write-Host "   Still creating tables..."
    Start-Sleep -Seconds 5
}

$exitCode = docker inspect -f '{{.State.ExitCode}}' agent_catalog_ddl_init

if ($exitCode -eq "0") {
    Write-Host ""
    Write-Host "✅ Agent Catalog is ready!"
    Write-Host ""
    Write-Host "📌 Services:"
    Write-Host "   MinIO Console  → http://localhost:9001  (minioadmin / minioadmin)"
    Write-Host "   Nessie API     → http://localhost:19120/api/v1/config"
    Write-Host ""
    Write-Host "📊 Tables created in namespaces:"
    Write-Host "   - nessie.catalog_core"
    Write-Host "   - nessie.catalog_curated"
    Write-Host "   - nessie.catalog_raw"
    Write-Host ""
    Write-Host "To stop:    docker compose down"
    Write-Host "To reset:   docker compose down -v"
} else {
    Write-Host ""
    Write-Host "❌ Table creation failed. Check logs:"
    Write-Host "   docker logs agent_catalog_ddl_init"
}
