# Deploy all PyFlink jobs to the Flink cluster

$FLINK_JM = "http://localhost:8081"
$JOBS_DIR = "flink\jobs"

Write-Host "Deploying PyFlink jobs to Flink cluster at $FLINK_JM" -ForegroundColor Cyan
Write-Host ""

function Submit-FlinkJob {
    param (
        [string]$JobFile
    )
    
    $jobName = [System.IO.Path]::GetFileNameWithoutExtension($JobFile)
    $jobBaseName = [System.IO.Path]::GetFileName($JobFile)
    
    Write-Host "Submitting $jobName..." -ForegroundColor Yellow
    
    # Copy job to Flink container and submit
    # Ensure destination directory exists and copy full path to avoid issues with spaces
    docker exec local-flink-jobmanager-1 mkdir -p /opt/flink/jobs 2>$null
    try {
        $hostPath = (Resolve-Path $JobFile).Path
    } catch {
        Write-Host "FAILED: $jobName - local file not found: $JobFile" -ForegroundColor Red
        return
    }
    docker cp "$hostPath" local-flink-jobmanager-1:/opt/flink/jobs/ 2>$null

    # Verify file exists in container
    $remotePath = "/opt/flink/jobs/$jobBaseName"
    $exists = docker exec local-flink-jobmanager-1 test -f $remotePath; if ($LASTEXITCODE -ne 0) { $exists = $false } else { $exists = $true }
    if (-not $exists) {
        Write-Host "FAILED: $jobName - file not found in container at $remotePath" -ForegroundColor Red
        Write-Host "  Tip: Verify you can access the file inside the JobManager container." -ForegroundColor Yellow
        return
    }

    # Check if Python is available in the Flink container
    docker exec local-flink-jobmanager-1 python --version >$null 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $jobName - Python not available inside Flink container" -ForegroundColor Red
        Write-Host "  To submit PyFlink jobs, ensure the Flink container has Python and PyFlink dependencies installed." -ForegroundColor Yellow
        Write-Host "  See docs/runbooks/phase2_quickstart.md (Deploy PyFlink jobs) for options." -ForegroundColor Gray
        return
    }

    # Submit job and capture output
    # Jobs directory is mounted at /opt/flink/jobs via docker-compose
    $submitOutput = docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -py $remotePath 2>&1
    if ($LASTEXITCODE -eq 0 -or $submitOutput -match "Job has been submitted") {
        Write-Host "SUCCESS: $jobName submitted" -ForegroundColor Green
    } else {
        Write-Host "FAILED: $jobName - submission failed" -ForegroundColor Red
        Write-Host "  Flink output: $submitOutput" -ForegroundColor Gray
        Write-Host "  Hint: If the error references missing Python files or modules, run jobs locally or build a Flink image with Python and required dependencies." -ForegroundColor Yellow
    }
    Write-Host ""
}

# Submit all jobs
Submit-FlinkJob "$JOBS_DIR\kpi_aggregator.py"
Submit-FlinkJob "$JOBS_DIR\bronze_writer.py"
Submit-FlinkJob "$JOBS_DIR\silver_order_state.py"
Submit-FlinkJob "$JOBS_DIR\gold_kpi_aggregates.py"
Submit-FlinkJob "$JOBS_DIR\alert_detector.py"

Write-Host "All jobs submitted! Check status at $FLINK_JM" -ForegroundColor Cyan
Write-Host "View running jobs at: http://localhost:8081" -ForegroundColor Gray
