#!/bin/bash
# Deploy all PyFlink jobs to the Flink cluster

FLINK_JM="http://localhost:8081"
JOBS_DIR="flink/jobs"

echo "🚀 Deploying PyFlink jobs to Flink cluster at $FLINK_JM"
echo ""

# Function to submit a Flink job
submit_job() {
    local job_file=$1
    local job_name=$(basename "$job_file" .py)
    local remote_path="/opt/flink/jobs/$(basename $job_file)"

    echo "Submitting $job_name..."

    # Jobs are already mounted via docker-compose, just verify
    docker exec local-flink-jobmanager-1 test -f $remote_path
    if [ $? -ne 0 ]; then
        echo "FAILED: $job_name - file not found in container at $remote_path"
        return 1
    fi

    # Check Python availability
    docker exec local-flink-jobmanager-1 python --version >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "FAILED: $job_name - Python not available inside Flink container"
        echo "Hint: build a Flink image with Python and PyFlink dependencies, or run jobs locally"
        return 1
    fi

    # Submit job (JARs are pre-installed in /opt/flink/lib/)
    submit_output=$(docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -py $remote_path 2>&1)
    if echo "$submit_output" | grep -q "Job has been submitted"; then
        echo "SUCCESS: $job_name submitted"
    else
        echo "FAILED: $job_name - submission failed"
        echo "Flink output: $submit_output"
    fi
    echo ""
}

# Submit all jobs
submit_job "$JOBS_DIR/kpi_aggregator.py"
submit_job "$JOBS_DIR/bronze_writer.py"
submit_job "$JOBS_DIR/silver_order_state.py"
submit_job "$JOBS_DIR/gold_kpi_aggregates.py"
submit_job "$JOBS_DIR/alert_detector.py"

echo "🎯 All jobs submitted! Check status at $FLINK_JM"
echo "   View running jobs: $FLINK_JM/#/job/running"
