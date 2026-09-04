#!/bin/bash
# =============================================================================
# Streaming ETL Framework - Post-Deployment Setup
# =============================================================================
# Run this after deploy.sh completes to:
#   1. Create RDS tables from generated DDL
#   2. Create Kafka topics from config
#   3. Start DMS replication
#   4. Start Glue streaming job
#   5. Create Athena tables
#   6. Generate test data via data generator Lambda
#
# Usage: ./post-deploy.sh [OPTIONS]
#   -s, --stack-name    Stack name (default: dq-etl)
#   -u, --use-case      Use case name (REQUIRED, e.g., vehicle-telemetry, healthcare-iot)
#   -r, --region        AWS region (default: us-east-1)
#   --skip-tables       Skip RDS table creation
#   --skip-topics       Skip Kafka topic creation
#   --skip-dms          Skip starting DMS task
#   --skip-glue         Skip starting Glue job
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================
STACK_NAME="${STACK_NAME:-dq-etl}"
REGION="${AWS_REGION:-us-east-1}"
USE_CASE=""
SNS_EMAILS="${SNS_EMAILS:-}"
SKIP_TABLES=false
SKIP_TOPICS=false
SKIP_DMS=false
SKIP_GLUE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${PROJECT_ROOT}/build"

# =============================================================================
# Colors and Logging
# =============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log_info()    { echo -e "${GREEN}[INFO]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}    $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC}   $1"; }
log_step()    { echo -e "${BLUE}[STEP]${NC}    ${BOLD}$1${NC}"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

# =============================================================================
# Resolve a Python 3.10+ interpreter (configurable via PYTHON_BIN)
# =============================================================================
resolve_python() {
    if [ -n "${PYTHON_BIN:-}" ]; then
        command -v "$PYTHON_BIN" &>/dev/null || { log_error "PYTHON_BIN='$PYTHON_BIN' not found"; exit 1; }
    elif command -v python3.10 &>/dev/null; then
        PYTHON_BIN="python3.10"
    elif command -v python3 &>/dev/null; then
        PYTHON_BIN="python3"
    else
        log_error "No python3 interpreter found. Install Python 3.10+ (see scripts/setup-ec2.sh)."
        exit 1
    fi
    if ! "$PYTHON_BIN" -c 'import sys; sys.exit(0 if sys.version_info[:2] >= (3, 10) else 1)' 2>/dev/null; then
        log_error "'$PYTHON_BIN' is older than Python 3.10. Set PYTHON_BIN to a 3.10+ interpreter."
        exit 1
    fi
}
resolve_python

# =============================================================================
# Parse Arguments
# =============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--stack-name) STACK_NAME="$2"; shift 2 ;;
            -u|--use-case) USE_CASE="$2"; shift 2 ;;
            -r|--region) REGION="$2"; shift 2 ;;
            --emails) SNS_EMAILS="$2"; shift 2 ;;
            --skip-tables) SKIP_TABLES=true; shift ;;
            --skip-topics) SKIP_TOPICS=true; shift ;;
            --skip-dms) SKIP_DMS=true; shift ;;
            --skip-glue) SKIP_GLUE=true; shift ;;
            *) log_error "Unknown option: $1"; exit 1 ;;
        esac
    done
}

# =============================================================================
# Validate Use Case
# =============================================================================
validate_use_case() {
    if [ -z "$USE_CASE" ]; then
        log_error "Missing required --use-case flag (e.g., vehicle-telemetry, healthcare-iot)"
        exit 1
    fi

    CONFIG_PATH="${PROJECT_ROOT}/examples/${USE_CASE}/config/tables.yaml"
    GENERATOR_PATH="${PROJECT_ROOT}/examples/${USE_CASE}/scripts/data_generator.py"

    if [ ! -f "$CONFIG_PATH" ]; then
        log_error "Config not found: examples/${USE_CASE}/config/tables.yaml"
        log_error "Available use cases:"
        for dir in "${PROJECT_ROOT}"/examples/*/; do
            [ -d "$dir" ] && log_error "  - $(basename "$dir")"
        done
        exit 1
    fi

    log_info "Use case: ${USE_CASE}"
    log_info "Config: examples/${USE_CASE}/config/tables.yaml"
}

# =============================================================================
# Utility Functions
# =============================================================================
invoke_lambda() {
    local function_name="$1"
    local payload="$2"
    local output_file="/tmp/lambda-response.json"
    
    aws lambda invoke \
        --function-name "$function_name" \
        --payload "$payload" \
        --cli-binary-format raw-in-base64-out \
        --region "$REGION" \
        "$output_file" > /dev/null 2>&1
    
    cat "$output_file"
}

get_stack_output() {
    local output_key="$1"
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue" \
        --output text 2>/dev/null
}

# =============================================================================
# Step 1: Create RDS Tables from Generated DDL
# =============================================================================
create_rds_tables() {
    if [ "$SKIP_TABLES" = true ]; then
        log_info "Skipping RDS table creation"
        return 0
    fi
    
    log_step "Creating RDS tables from generated DDL..."
    
    local sql_runner="${STACK_NAME}-sql-runner"
    local ddl_file="${BUILD_DIR}/create_tables.sql"
    
    if [ ! -f "$ddl_file" ]; then
        log_warn "Generated DDL not found at ${ddl_file}. Running config compiler..."
        "$PYTHON_BIN" -m src.config_compiler --config "$CONFIG_PATH" --output "$BUILD_DIR" || {
            log_error "Config compilation failed."
            exit 1
        }
    fi
    
    # Read DDL and execute each statement via SQL Runner Lambda
    # Split on semicolons and execute each non-empty statement
    local statement=""
    while IFS= read -r line; do
        # Skip empty lines and comments
        [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]] && continue
        statement="${statement} ${line}"
        if [[ "$line" == *";" ]]; then
            # Clean up the statement
            statement=$(echo "$statement" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')
            if [ -n "$statement" ]; then
                log_info "Executing: ${statement:0:80}..."
                local payload
                payload=$(printf '{"sql": "%s"}' "$(echo "$statement" | sed 's/"/\\"/g')")
                invoke_lambda "$sql_runner" "$payload"
                echo ""
            fi
            statement=""
        fi
    done < "$ddl_file"
    
    log_success "RDS tables created from generated DDL"
}

# =============================================================================
# Step 2: Create Kafka Topics from Config
# =============================================================================
create_kafka_topics() {
    if [ "$SKIP_TOPICS" = true ]; then
        log_info "Skipping Kafka topic creation"
        return 0
    fi
    
    log_step "Creating Kafka topics from config..."
    
    local kafka_admin="${STACK_NAME}-kafka-admin"
    
    log_info "Syncing topics from tables.yaml..."
    local result
    result=$(invoke_lambda "$kafka_admin" '{"action": "sync_from_config"}')
    echo "$result"
    echo ""
    
    log_info "Listing all topics..."
    invoke_lambda "$kafka_admin" '{"action": "list"}'
    echo ""
    
    log_success "Kafka topics created"
}

# =============================================================================
# Step 3: Start DMS Replication Task
# =============================================================================
start_dms_task() {
    if [ "$SKIP_DMS" = true ]; then
        log_info "Skipping DMS task start"
        return 0
    fi
    
    log_step "Starting DMS replication task..."
    
    local dms_task_arn
    dms_task_arn=$(get_stack_output "DMSTaskArn")
    
    if [ -z "$dms_task_arn" ]; then
        log_warn "DMS task ARN not found"
        return 0
    fi
    
    # Get replication instance ARN
    local replication_instance_arn
    replication_instance_arn=$(aws dms describe-replication-instances \
        --filters Name=replication-instance-id,Values="${STACK_NAME}-dms" \
        --region "$REGION" \
        --query 'ReplicationInstances[0].ReplicationInstanceArn' \
        --output text 2>/dev/null || echo "")
    
    # Get source and target endpoint ARNs
    local source_endpoint_arn target_endpoint_arn
    source_endpoint_arn=$(aws dms describe-endpoints \
        --filters Name=endpoint-id,Values="${STACK_NAME}-source-postgres" \
        --region "$REGION" \
        --query 'Endpoints[0].EndpointArn' \
        --output text 2>/dev/null || echo "")
    
    target_endpoint_arn=$(aws dms describe-endpoints \
        --filters Name=endpoint-id,Values="${STACK_NAME}-target-msk" \
        --region "$REGION" \
        --query 'Endpoints[0].EndpointArn' \
        --output text 2>/dev/null || echo "")
    
    # Test connections and wait for them to succeed
    if [ -n "$replication_instance_arn" ] && [ -n "$source_endpoint_arn" ]; then
        log_info "Testing DMS source connection..."
        aws dms test-connection \
            --replication-instance-arn "$replication_instance_arn" \
            --endpoint-arn "$source_endpoint_arn" \
            --region "$REGION" > /dev/null 2>&1 || true
    fi
    
    if [ -n "$replication_instance_arn" ] && [ -n "$target_endpoint_arn" ]; then
        log_info "Testing DMS target connection..."
        aws dms test-connection \
            --replication-instance-arn "$replication_instance_arn" \
            --endpoint-arn "$target_endpoint_arn" \
            --region "$REGION" > /dev/null 2>&1 || true
    fi
    
    # Wait for both connections to be successful (up to 3 minutes)
    log_info "Waiting for DMS connection tests to complete..."
    local max_wait=180
    local elapsed=0
    local interval=15
    local source_status="testing"
    local target_status="testing"
    
    while [ $elapsed -lt $max_wait ]; do
        # Check connection statuses
        source_status=$(aws dms describe-connections \
            --region "$REGION" \
            --query "Connections[?EndpointIdentifier=='${STACK_NAME}-source-postgres'].Status" \
            --output text 2>/dev/null || echo "unknown")
        
        target_status=$(aws dms describe-connections \
            --region "$REGION" \
            --query "Connections[?EndpointIdentifier=='${STACK_NAME}-target-msk'].Status" \
            --output text 2>/dev/null || echo "unknown")
        
        echo -ne "\r  Source: ${source_status}, Target: ${target_status} (${elapsed}s)    "
        
        if [ "$source_status" = "successful" ] && [ "$target_status" = "successful" ]; then
            echo ""
            log_success "Both DMS connections successful"
            break
        fi
        
        if [ "$source_status" = "failed" ] || [ "$target_status" = "failed" ]; then
            echo ""
            log_error "DMS connection test failed - Source: ${source_status}, Target: ${target_status}"
            return 1
        fi
        
        sleep $interval
        elapsed=$((elapsed + interval))
    done
    
    if [ $elapsed -ge $max_wait ]; then
        echo ""
        log_warn "Timeout waiting for connections. Attempting to start anyway..."
    fi
    
    local task_status
    task_status=$(aws dms describe-replication-tasks \
        --filters Name=replication-task-arn,Values="$dms_task_arn" \
        --region "$REGION" \
        --query 'ReplicationTasks[0].Status' \
        --output text 2>/dev/null || echo "unknown")
    
    log_info "Current DMS task status: ${task_status}"
    
    if [ "$task_status" = "stopped" ] || [ "$task_status" = "ready" ]; then
        log_info "Starting DMS task..."
        if aws dms start-replication-task \
            --replication-task-arn "$dms_task_arn" \
            --start-replication-task-type start-replication \
            --region "$REGION" > /tmp/dms-start.json 2>&1; then
            log_success "DMS task started"
        else
            log_error "Failed to start DMS task:"
            cat /tmp/dms-start.json
        fi
    elif [ "$task_status" = "failed" ]; then
        log_warn "DMS task previously failed, restarting with resume-processing..."
        if aws dms start-replication-task \
            --replication-task-arn "$dms_task_arn" \
            --start-replication-task-type resume-processing \
            --region "$REGION" > /tmp/dms-start.json 2>&1; then
            log_success "DMS task resumed"
        else
            log_warn "Resume failed, trying reload-target..."
            if aws dms start-replication-task \
                --replication-task-arn "$dms_task_arn" \
                --start-replication-task-type reload-target \
                --region "$REGION" > /tmp/dms-start.json 2>&1; then
                log_success "DMS task restarted with reload-target"
            else
                log_warn "Reload-target failed, trying start-replication..."
                aws dms start-replication-task \
                    --replication-task-arn "$dms_task_arn" \
                    --start-replication-task-type start-replication \
                    --region "$REGION" > /tmp/dms-start.json 2>&1 || {
                        log_error "All DMS start attempts failed:"
                        cat /tmp/dms-start.json
                    }
            fi
        fi
    elif [ "$task_status" = "running" ]; then
        log_info "DMS task already running"
    elif [ "$task_status" = "starting" ]; then
        log_info "DMS task is starting..."
    else
        log_warn "DMS task in state: ${task_status}, attempting start..."
        aws dms start-replication-task \
            --replication-task-arn "$dms_task_arn" \
            --start-replication-task-type start-replication \
            --region "$REGION" > /tmp/dms-start.json 2>&1 || {
                log_warn "Start failed, trying resume-processing..."
                aws dms start-replication-task \
                    --replication-task-arn "$dms_task_arn" \
                    --start-replication-task-type resume-processing \
                    --region "$REGION" > /tmp/dms-start.json 2>&1 || true
            }
    fi
}

# =============================================================================
# Step 4: Start Glue Streaming Job
# =============================================================================
start_glue_job() {
    if [ "$SKIP_GLUE" = true ]; then
        log_info "Skipping Glue job start"
        return 0
    fi
    
    log_step "Starting Glue streaming job..."
    
    local glue_job_name
    glue_job_name=$(get_stack_output "GlueJobName")
    
    if [ -z "$glue_job_name" ]; then
        log_warn "Glue job name not found"
        return 0
    fi
    
    # Stop any running job first (needed to pick up new script changes)
    local running_jobs
    running_jobs=$(aws glue get-job-runs \
        --job-name "$glue_job_name" \
        --region "$REGION" \
        --query "JobRuns[?JobRunState=='RUNNING'].Id" \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$running_jobs" ]; then
        log_info "Stopping existing Glue job run to pick up latest code..."
        aws glue batch-stop-job-run \
            --job-name "$glue_job_name" \
            --job-run-ids $running_jobs \
            --region "$REGION" > /dev/null 2>&1 || true
        
        # Wait for job to actually stop (up to 3 minutes)
        log_info "Waiting for Glue job to stop..."
        local max_wait=180
        local elapsed=0
        local interval=15
        while [ $elapsed -lt $max_wait ]; do
            local current_state
            current_state=$(aws glue get-job-runs \
                --job-name "$glue_job_name" \
                --region "$REGION" \
                --query "JobRuns[0].JobRunState" \
                --output text 2>/dev/null || echo "UNKNOWN")
            
            echo -ne "\r  Glue job state: ${current_state} (${elapsed}s)    "
            
            if [ "$current_state" != "RUNNING" ] && [ "$current_state" != "STOPPING" ]; then
                echo ""
                log_info "Glue job stopped (state: ${current_state})"
                break
            fi
            
            sleep $interval
            elapsed=$((elapsed + interval))
        done
        
        if [ $elapsed -ge $max_wait ]; then
            echo ""
            log_warn "Timeout waiting for Glue job to stop. Attempting start anyway..."
        fi

        # Glue needs a brief pause after stop before accepting a new start
        log_info "Waiting 15s for Glue to finalize stop..."
        sleep 15
    fi
    
    log_info "Starting Glue job: ${glue_job_name}"
    local start_attempts=0
    local max_attempts=3
    while [ $start_attempts -lt $max_attempts ]; do
        if aws glue start-job-run \
            --job-name "$glue_job_name" \
            --region "$REGION" > /dev/null 2>&1; then
            log_success "Glue job started"
            return 0
        fi
        start_attempts=$((start_attempts + 1))
        if [ $start_attempts -lt $max_attempts ]; then
            log_warn "Start attempt ${start_attempts} failed, retrying in 15s..."
            sleep 15
        fi
    done
    log_error "Failed to start Glue job after ${max_attempts} attempts"
}

# =============================================================================
# Step 5: Create Athena Tables via Lambda
# =============================================================================
create_athena_tables() {
    log_step "Creating Athena tables for Delta Lake..."
    
    local athena_creator="${STACK_NAME}-athena-table-creator"
    
    log_info "Invoking Athena table creator Lambda..."
    local result
    result=$(invoke_lambda "$athena_creator" '{"action": "create_all"}')
    echo "$result"
    echo ""
    
    log_success "Athena tables created"
    
    local database="${STACK_NAME//-/_}_db"
    log_info "Query tables in Athena: SELECT * FROM ${database}.<table_name> LIMIT 10;"
}

# =============================================================================
# Step 6: Generate Test Data via Data Generator Lambda
# =============================================================================
generate_test_data() {
    log_step "Generating test data via data generator Lambda..."
    
    local generator="${STACK_NAME}-data-generator"
    
    # Check if generator Lambda exists
    if ! aws lambda get-function --function-name "$generator" --region "$REGION" &>/dev/null; then
        log_warn "Data generator Lambda not found: ${generator}"
        log_info "You can generate data manually after deploying the generator."
        return 0
    fi
    
    # Seed reference data first
    log_info "Seeding reference data..."
    invoke_lambda "$generator" '{"action": "seed"}'
    echo ""
    
    # Generate a burst of test data
    log_info "Generating burst of test data..."
    invoke_lambda "$generator" '{"action": "burst", "records": 100}'
    echo ""
    
    log_success "Test data generated"
}

# =============================================================================
# Display Summary
# =============================================================================
display_summary() {
    log_step "Post-Deployment Summary"
    echo ""
    
    local rds_endpoint delta_bucket glue_job
    rds_endpoint=$(get_stack_output "RDSEndpoint")
    delta_bucket=$(get_stack_output "DeltaBucket")
    glue_job=$(get_stack_output "GlueJobName")
    
    echo "Stack Name: ${STACK_NAME}"
    echo "Use Case:   ${USE_CASE}"
    echo "Region:     ${REGION}"
    echo ""
    [ -n "$rds_endpoint" ] && echo "RDS Endpoint: ${rds_endpoint}"
    [ -n "$delta_bucket" ] && echo "Delta Bucket: ${delta_bucket}"
    [ -n "$glue_job" ] && echo "Glue Job: ${glue_job}"
    echo ""
    
    echo "Next Steps:"
    echo "  1. Check Glue job status in AWS Console"
    echo "  2. Query Delta Lake tables in Athena"
    echo "  3. Check quarantine bucket for DQ failures:"
    echo "     aws s3 ls s3://${STACK_NAME}-quarantine-\${AWS_ACCOUNT_ID}/ --recursive"
    echo "  4. Generate more data:"
    echo "     aws lambda invoke --function-name ${STACK_NAME}-data-generator --payload '{\"action\": \"burst\", \"records\": 100}' /tmp/out.json"
    echo ""
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo -e "${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║   Streaming ETL Framework - Post-Deployment Setup          ║${NC}"
    echo -e "${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    parse_args "$@"
    validate_use_case
    
    log_info "Stack Name: ${STACK_NAME}"
    log_info "Use Case: ${USE_CASE}"
    log_info "Region: ${REGION}"
    echo ""
    
    create_rds_tables
    echo ""
    
    create_kafka_topics
    echo ""
    
    start_dms_task
    echo ""
    
    start_glue_job
    echo ""
    
    create_athena_tables
    echo ""
    
    generate_test_data
    echo ""
    
    subscribe_sns_emails
    echo ""

    display_summary
    
    log_success "Post-deployment setup complete!"
    echo ""
}

# =============================================================================
# Step 7: Subscribe SNS Email Notifications (if --emails provided)
# =============================================================================
subscribe_sns_emails() {
    if [ -z "$SNS_EMAILS" ]; then
        return 0
    fi
    
    log_step "Subscribing emails to DQ alerts SNS topic..."
    
    local sns_topic_arn
    sns_topic_arn=$(get_stack_output "DQAlertsTopicArn")
    
    if [ -z "$sns_topic_arn" ]; then
        log_warn "SNS topic ARN not found in stack outputs"
        return 0
    fi
    
    IFS=',' read -ra EMAILS <<< "$SNS_EMAILS"
    for email in "${EMAILS[@]}"; do
        email=$(echo "$email" | xargs)  # trim whitespace
        if [ -n "$email" ]; then
            if aws sns subscribe \
                --topic-arn "$sns_topic_arn" \
                --protocol email \
                --notification-endpoint "$email" \
                --region "$REGION" > /dev/null 2>&1; then
                log_success "Subscribed: ${email}"
            else
                log_warn "Failed to subscribe: ${email}"
            fi
        fi
    done
    
    log_info "Check email inboxes and click 'Confirm subscription' to activate alerts"
}

main "$@"
