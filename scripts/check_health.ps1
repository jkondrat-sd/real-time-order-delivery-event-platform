# Service Health Check Script
# Verifies all Docker services are running and healthy

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 59) -ForegroundColor Cyan
Write-Host "🏥 Service Health Check" -ForegroundColor Cyan
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 59) -ForegroundColor Cyan
Write-Host ""

$services = @(
    @{Name="redpanda"; Container="local-redpanda-1"; Port=29092; Check="rpk cluster health"},
    @{Name="schema-registry"; Container="local-schema-registry-1"; Port=18081; Check=$null},
    @{Name="flink-jobmanager"; Container="local-flink-jobmanager-1"; Port=8081; Check=$null},
    @{Name="flink-taskmanager"; Container="local-flink-taskmanager-1"; Port=$null; Check=$null},
    @{Name="minio"; Container="local-minio-1"; Port=9001; Check=$null},
    @{Name="hive-metastore"; Container="local-hive-metastore-1"; Port=9083; Check=$null},
    @{Name="trino"; Container="local-trino-1"; Port=8082; Check=$null},
    @{Name="grafana"; Container="local-grafana-1"; Port=3000; Check=$null},
    @{Name="prometheus"; Container="local-prometheus-1"; Port=9090; Check=$null},
    @{Name="duckdb/jupyter"; Container="local-duckdb-1"; Port=8888; Check=$null},
    @{Name="redpanda-console"; Container="local-redpanda-console-1"; Port=8080; Check=$null}
)

$allHealthy = $true

foreach ($service in $services) {
    $name = $service.Name
    $containerName = $service.Container
    $status = docker inspect -f '{{.State.Status}}' $containerName 2>$null
    
    if ($status -eq "running") {
        # Check if service has custom health check
        if ($service.Check) {
            $healthCheck = (docker exec $containerName $service.Check 2>$null) -join "`n"
            # Treat the service as healthy if output contains a true flag for health
            if ($healthCheck -match "(?i)healthy.*true" -or $healthCheck -match "(?i)\btrue\b") {
                Write-Host "✅ $name" -ForegroundColor Green -NoNewline
                Write-Host " (running + healthy)" -ForegroundColor Gray
            } else {
                Write-Host "⚠️  $name" -ForegroundColor Yellow -NoNewline
                Write-Host " (running but not healthy yet)" -ForegroundColor Gray
                $allHealthy = $false
            }
        } else {
            Write-Host "✅ $name" -ForegroundColor Green -NoNewline
            if ($service.Port) {
                Write-Host " (port $($service.Port))" -ForegroundColor Gray
            } else {
                Write-Host "" -ForegroundColor Gray
            }
        }
    } elseif ($status) {
        Write-Host "❌ $name" -ForegroundColor Red -NoNewline
        Write-Host " (status: $status)" -ForegroundColor Gray
        $allHealthy = $false
    } else {
        Write-Host "❌ $name" -ForegroundColor Red -NoNewline
        Write-Host " (not found)" -ForegroundColor Gray
        $allHealthy = $false
    }
}

Write-Host ""
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 59) -ForegroundColor Cyan

if ($allHealthy) {
    Write-Host "🎉 All services healthy!" -ForegroundColor Green
} else {
    Write-Host "⚠️  Some services need attention" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "  • Wait 1-2 minutes for services to fully initialize" -ForegroundColor White
    Write-Host "  • Check logs: docker logs <service-name>" -ForegroundColor White
    Write-Host "  • Restart services: python -m tasks down && python -m tasks up" -ForegroundColor White
}

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 59) -ForegroundColor Cyan
