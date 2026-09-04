#!/bin/bash
# =============================================================================
# Streaming ETL Framework - Teardown Script
# =============================================================================
# Complete cleanup of all resources created by the stack.
# Continues on errors to ensure maximum cleanup.
#
# Usage: ./teardown.sh [OPTIONS]
#   -s, --stack-name    Stack name (default: dq-etl)
#   -r, --region        AWS region (default: us-east-1)
#   -f, --force         Skip confirmation prompt
#   -h, --help          Show this help message
# =============================================================================

set +e

STACK_NAME="${STACK_NAME:-dq-etl}"
REGION="${AWS_REGION:-us-east-1}"
FORCE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

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

# Resolve a Python interpreter (configurable via PYTHON_BIN). Non-fatal: teardown
# should keep running even if python is missing (the only use is counting S3
# object versions, which degrades gracefully to 0).
if [ -z "${PYTHON_BIN:-}" ]; then
    if command -v python3.10 &>/dev/null; then PYTHON_BIN="python3.10"
    elif command -v python3 &>/dev/null; then PYTHON_BIN="python3"
    else PYTHON_BIN="python3"; fi
fi

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --force|-f) FORCE=true; shift ;;
            --stack-name|-s) STACK_NAME="$2"; shift 2 ;;
            --region|-r) REGION="$2"; shift 2 ;;
            --help|-h) show_help; exit 0 ;;
            *) log_error "Unknown option: $1"; show_help; exit 1 ;;
        esac
    done
}

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo "  --force, -f           Skip confirmation prompt"
    echo "  --stack-name, -s      CloudFormation stack name (default: dq-etl)"
    echo "  --region, -r          AWS region (default: us-east-1)"
    echo "  --help, -h            Show this help message"
}

check_stack_exists() {
    aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" &>/dev/null
    return $?
}

stop_glue_jobs() {
    log_step "Stopping Glue streaming jobs..."
    local glue_job="${STACK_NAME}-streaming-job"
    
    if ! aws glue get-job --job-name "$glue_job" --region "$REGION" &>/dev/null; then
        log_info "Glue job '${glue_job}' does not exist, skipping"
        return 0
    fi
    
    local running_jobs
    running_jobs=$(aws glue get-job-runs --job-name "$glue_job" --region "$REGION" \
        --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING'].JobRunId" \
        --output text 2>/dev/null || echo "")
    
    if [ -z "$running_jobs" ] || [ "$running_jobs" == "None" ]; then
        log_info "No running Glue job runs found"
        return 0
    fi
    
    log_info "Stopping running Glue jobs..."
    for job_run in $running_jobs; do
        aws glue batch-stop-job-run --job-name "$glue_job" --job-run-ids "$job_run" --region "$REGION" 2>/dev/null || {
            log_warn "Failed to stop Glue job run: ${job_run}"
        }
    done
    
    log_info "Waiting 30s for Glue jobs to stop..."
    sleep 30
    log_success "Glue jobs stopped"
}

stop_dms_task() {
    log_step "Stopping DMS replication task..."
    
    local dms_task_arn
    dms_task_arn=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='DMSTaskArn'].OutputValue" --output text 2>/dev/null || echo "")
    
    if [ -z "$dms_task_arn" ] || [ "$dms_task_arn" == "None" ]; then
        log_info "DMS task ARN not found, skipping"
        return 0
    fi
    
    local dms_status
    dms_status=$(aws dms describe-replication-tasks \
        --filters Name=replication-task-arn,Values="$dms_task_arn" \
        --region "$REGION" \
        --query "ReplicationTasks[0].Status" --output text 2>/dev/null || echo "")
    
    if [ "$dms_status" == "running" ] || [ "$dms_status" == "starting" ]; then
        log_info "Stopping DMS replication task (status: ${dms_status})..."
        aws dms stop-replication-task --replication-task-arn "$dms_task_arn" --region "$REGION" 2>/dev/null || {
            log_warn "Failed to stop DMS task"
        }
        log_info "Waiting 60s for DMS task to stop..."
        sleep 60
    else
        log_info "DMS task status: ${dms_status:-unknown}"
    fi
    
    log_success "DMS task stopped"
}

empty_bucket() {
    local bucket=$1
    if [ -z "$bucket" ] || [ "$bucket" == "None" ]; then
        return 0
    fi
    
    if ! aws s3api head-bucket --bucket "$bucket" --region "$REGION" 2>/dev/null; then
        log_info "Bucket '${bucket}' does not exist, skipping"
        return 0
    fi
    
    log_info "Emptying bucket: ${bucket}"
    aws s3 rm "s3://${bucket}" --recursive --region "$REGION" 2>/dev/null || {
        log_warn "Failed to empty bucket: ${bucket}"
    }

    # Versioned buckets need BOTH object versions and delete markers removed, in
    # pages: CloudFormation cannot delete a bucket that still holds either, and a
    # single list call returns at most 1000 entries. `aws s3 rm` above only
    # removes current versions (and actually ADDS delete markers), so this loop
    # is what makes the bucket genuinely empty.
    local payload="${TMPDIR:-/tmp}/teardown-delete-$$.json"
    local page=0
    while :; do
        aws s3api list-object-versions --bucket "$bucket" --region "$REGION" \
            --max-items 500 --output json 2>/dev/null \
            | "$PYTHON_BIN" -c '
import json
import sys

try:
    data = json.load(sys.stdin)
except ValueError:
    sys.exit(0)

entries = data.get("Versions") or []
entries += data.get("DeleteMarkers") or []
if not entries:
    sys.exit(0)

payload = {
    "Objects": [
        {"Key": e["Key"], "VersionId": e["VersionId"]} for e in entries
    ],
    "Quiet": True,
}
sys.stdout.write(json.dumps(payload))
' > "$payload" 2>/dev/null

        # No payload written means nothing left to delete.
        if [ ! -s "$payload" ]; then
            break
        fi

        aws s3api delete-objects --bucket "$bucket" --region "$REGION" \
            --delete "file://${payload}" >/dev/null 2>&1 || {
                log_warn "Failed to delete object versions in: ${bucket}"
                break
            }

        page=$((page + 1))
        if [ "$page" -ge 200 ]; then
            log_warn "Stopped after ${page} pages in ${bucket}; re-run teardown if needed"
            break
        fi
    done
    rm -f "$payload"

    if [ "$page" -gt 0 ]; then
        log_info "Removed versions/delete markers from ${bucket} (${page} page(s))"
    fi
}

empty_buckets() {
    log_step "Emptying S3 buckets..."
    
    local buckets
    buckets=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
        --query "Stacks[0].Outputs[?contains(OutputKey, 'Bucket')].OutputValue" --output text 2>/dev/null || echo "")
    
    if [ -z "$buckets" ] || [ "$buckets" == "None" ]; then
        log_info "No buckets found in stack outputs"
        return 0
    fi
    
    # Also discover buckets by the stack-name prefix. Some buckets (e.g. the
    # versioned S3 access-logs bucket) are NOT exported as stack outputs, so
    # relying on outputs alone leaves them non-empty and the stack delete fails.
    local prefix_buckets
    prefix_buckets=$(aws s3api list-buckets \
        --query "Buckets[?starts_with(Name, '${STACK_NAME}-')].Name" \
        --output text 2>/dev/null || echo "")

    for bucket in $buckets $prefix_buckets; do
        empty_bucket "$bucket"
    done
    
    log_success "S3 buckets emptied"
}

delete_lambda_layers() {
    log_step "Cleaning up Lambda layers..."
    
    local layer_names=(
        "${STACK_NAME}-psycopg2"
        "${STACK_NAME}-kafka-python"
        "${STACK_NAME}-pyyaml"
    )
    
    for layer_name in "${layer_names[@]}"; do
        local versions
        versions=$(aws lambda list-layer-versions \
            --layer-name "$layer_name" \
            --region "$REGION" \
            --query 'LayerVersions[].Version' \
            --output text 2>/dev/null || echo "")
        
        if [ -n "$versions" ] && [ "$versions" != "None" ]; then
            for version in $versions; do
                log_info "Deleting layer ${layer_name} version ${version}..."
                aws lambda delete-layer-version \
                    --layer-name "$layer_name" \
                    --version-number "$version" \
                    --region "$REGION" 2>/dev/null || {
                        log_warn "Failed to delete layer ${layer_name} v${version}"
                    }
            done
        fi
    done
    
    log_success "Lambda layers cleaned up"
}

# Lambda functions in a VPC leave managed ENIs behind for a while after the
# function is deleted. Those ENIs hold the subnet and security group, so the
# stack delete fails with "has dependencies and cannot be deleted". Release the
# detached ones and give AWS a bounded window to reclaim the rest.
release_orphaned_enis() {
    local vpc_id
    vpc_id=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" --region "$REGION" \
        --query "StackResources[?ResourceType=='AWS::EC2::VPC'].PhysicalResourceId | [0]" \
        --output text 2>/dev/null || echo "")

    if [ -z "$vpc_id" ] || [ "$vpc_id" == "None" ]; then
        log_info "VPC not found in stack resources, skipping ENI cleanup"
        return 0
    fi

    log_info "Releasing orphaned network interfaces in ${vpc_id}..."
    local attempt
    for attempt in $(seq 1 10); do
        local available
        available=$(aws ec2 describe-network-interfaces --region "$REGION" \
            --filters "Name=vpc-id,Values=${vpc_id}" \
            --query "NetworkInterfaces[?Status=='available'].NetworkInterfaceId" \
            --output text 2>/dev/null || echo "")

        local eni
        for eni in $available; do
            aws ec2 delete-network-interface --network-interface-id "$eni" --region "$REGION" 2>/dev/null \
                && log_info "Deleted ENI ${eni}"
        done

        local in_use
        in_use=$(aws ec2 describe-network-interfaces --region "$REGION" \
            --filters "Name=vpc-id,Values=${vpc_id}" \
            --query "NetworkInterfaces[?Status!='available'].NetworkInterfaceId" \
            --output text 2>/dev/null || echo "")

        if [ -z "$in_use" ] || [ "$in_use" == "None" ]; then
            log_success "No network interfaces remaining"
            return 0
        fi

        log_info "ENIs still in use, waiting 60s (attempt ${attempt}/10)..."
        sleep 60
    done

    log_warn "Some ENIs are still attached; AWS usually releases them within ~40 minutes"
}

delete_stack() {
    log_step "Deleting CloudFormation stack: ${STACK_NAME}"

    if ! check_stack_exists; then
        log_info "Stack '${STACK_NAME}' does not exist"
        return 0
    fi

    local attempt
    for attempt in 1 2; do
        aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null || {
            log_error "Failed to initiate stack deletion"
            return 1
        }

        log_info "Waiting for stack deletion (this may take 15-30 minutes)..."
        if aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null; then
            log_success "Stack deleted"
            return 0
        fi

        # Gone entirely? Then the wait failed only because the stack disappeared.
        if ! check_stack_exists; then
            log_success "Stack deleted"
            return 0
        fi

        local status
        status=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
            --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "UNKNOWN")

        if [ "$attempt" -eq 1 ] && [ "$status" == "DELETE_FAILED" ]; then
            log_warn "Stack deletion failed. Failed resources:"
            aws cloudformation describe-stack-events --stack-name "$STACK_NAME" --region "$REGION" \
                --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].[LogicalResourceId,ResourceStatusReason]" \
                --output text 2>/dev/null | head -10

            log_step "Attempting automatic recovery..."
            # The two things that block deletion in practice: buckets that refilled
            # while the pipeline drained, and Lambda ENIs pinning the subnet/SG.
            empty_buckets
            release_orphaned_enis
            log_info "Retrying stack deletion..."
            continue
        fi

        log_warn "Stack status: ${status}"
        log_warn "Check the AWS Console for details"
        return 1
    done
}

cleanup_local() {
    log_step "Cleaning up local build artifacts..."
    
    if [ -d "${PROJECT_ROOT}/build" ]; then
        rm -rf "${PROJECT_ROOT}/build"
        log_info "Removed build/ directory"
    fi
    
    log_success "Local cleanup complete"
}

main() {
    parse_args "$@"
    
    echo ""
    echo -e "${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║     Streaming ETL Framework - Stack Teardown               ║${NC}"
    echo -e "${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    log_warn "This will DELETE all resources in stack: ${STACK_NAME}"
    log_warn "Region: ${REGION}"
    echo ""
    
    if [ "$FORCE" != true ]; then
        read -p "Are you sure? (yes/no): " confirm
        if [ "$confirm" != "yes" ]; then
            log_info "Teardown cancelled"
            exit 0
        fi
    fi
    
    echo ""
    
    stop_glue_jobs
    echo ""
    
    stop_dms_task
    echo ""
    
    empty_buckets
    echo ""
    
    delete_lambda_layers
    echo ""
    
    delete_stack
    echo ""
    
    cleanup_local
    echo ""
    
    log_success "Teardown complete for stack: ${STACK_NAME}"
    echo ""
}

main "$@"
