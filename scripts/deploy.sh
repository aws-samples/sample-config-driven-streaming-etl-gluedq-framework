#!/bin/bash
# =============================================================================
# Streaming ETL Framework - One-Click Deployment Script
# =============================================================================
# Fully automated deployment for the streaming ETL pipeline.
# Deploys CloudFormation stack, uploads assets, creates tables/topics,
# and starts the data pipeline.
#
# COMPLIANCE NOTE: The bundled examples generate synthetic PII/PHI to demonstrate
# the mask_pii transform. If you deploy this pipeline against real personal or
# patient data, you are responsible for your own HIPAA/GDPR safeguards, including
# a Business Associate Agreement (BAA) with AWS where required.
# See https://aws.amazon.com/compliance/hipaa-compliance/
#
# Usage: ./deploy.sh [OPTIONS]
#   -h, --help          Show this help message
#   -s, --stack-name    Stack name (default: dq-etl)
#   -u, --use-case      Use case name (REQUIRED, e.g., vehicle-telemetry, healthcare-iot)
#   -r, --region        AWS region (default: us-east-1)
#   --start-glue        Start Glue streaming job after deployment
#   --skip-stack        Skip CloudFormation deployment (use existing stack)
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================
STACK_NAME="${STACK_NAME:-dq-etl}"
REGION="${AWS_REGION:-us-east-1}"
USE_CASE=""
RDS_PASSWORD="${RDS_PASSWORD:-}"
MSK_PASSWORD="${MSK_PASSWORD:-}"
USE_PREVIOUS_RDS=false
USE_PREVIOUS_MSK=false
START_GLUE=false
SKIP_STACK=false
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
# The local scripts only run pure-Python helpers (config validator/compiler,
# JSON param building) which need Python >= 3.10 and PyYAML. The deploy-host
# Python is independent of the Glue 6.0 job runtime (Python 3.13 — served by
# the cp313 wheel uploaded to S3) and of the Lambda runtimes (python3.10 —
# served by the cp310 layer wheels). Prefer python3.10 by name
# but fall back to any python3 that satisfies the minimum version so the
# scripts are not brittle to the interpreter's exact binary name.
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
# Usage Help
# =============================================================================
show_help() {
    echo -e "${BOLD}Streaming ETL Framework - One-Click Deployment${NC}"
    echo ""
    echo -e "${BOLD}USAGE:${NC}"
    echo "    ./deploy.sh --use-case <name> [OPTIONS]"
    echo ""
    echo -e "${BOLD}REQUIRED:${NC}"
    echo "    -u, --use-case      Use case name (e.g., vehicle-telemetry, healthcare-iot)"
    echo ""
    echo -e "${BOLD}OPTIONS:${NC}"
    echo "    -h, --help          Show this help message"
    echo "    -s, --stack-name    CloudFormation stack name (default: dq-etl)"
    echo "    -r, --region        AWS region (default: us-east-1)"
    echo "    --rds-password      RDS PostgreSQL password (prompted if not provided)"
    echo "    --msk-password      MSK SASL/SCRAM password (prompted if not provided)"
    echo "    --start-glue        Start Glue streaming job after deployment"
    echo "    --skip-stack        Skip CloudFormation deployment"
    echo ""
    echo -e "${BOLD}ENVIRONMENT VARIABLES:${NC}"
    echo "    RDS_PASSWORD        RDS PostgreSQL password (alternative to --rds-password)"
    echo "    MSK_PASSWORD        MSK SASL/SCRAM password (alternative to --msk-password)"
    echo ""
    echo -e "${BOLD}EXAMPLES:${NC}"
    echo "    ./deploy.sh --use-case vehicle-telemetry"
    echo "    ./deploy.sh --stack-name fleet-demo --use-case vehicle-telemetry"
    echo "    ./deploy.sh --stack-name health-demo --use-case healthcare-iot --region us-west-2"
    echo ""
    exit 0
}

# =============================================================================
# Parse Arguments
# =============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help) show_help ;;
            -s|--stack-name) STACK_NAME="$2"; shift 2 ;;
            -u|--use-case) USE_CASE="$2"; shift 2 ;;
            -r|--region) REGION="$2"; shift 2 ;;
            --rds-password) RDS_PASSWORD="$2"; shift 2 ;;
            --msk-password) MSK_PASSWORD="$2"; shift 2 ;;
            --start-glue) START_GLUE=true; shift ;;
            --skip-stack) SKIP_STACK=true; shift ;;
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

    if [ -f "$GENERATOR_PATH" ]; then
        log_info "Generator: examples/${USE_CASE}/scripts/data_generator.py"
    else
        log_warn "No data generator found for use case: ${USE_CASE}"
    fi
}

# =============================================================================
# Prompt for Passwords (if not provided via CLI or env vars)
# =============================================================================
prompt_passwords() {
    if [ "$SKIP_STACK" = true ]; then
        return 0
    fi

    # For stack updates, use previous values if passwords not provided
    local stack_status
    stack_status=$(stack_exists 2>/dev/null || echo "DOES_NOT_EXIST")

    if [[ "$stack_status" != "DOES_NOT_EXIST" ]] && [[ "$stack_status" != *"ROLLBACK"* ]]; then
        # Existing stack — use previous values unless explicitly provided
        if [ -z "$RDS_PASSWORD" ]; then
            USE_PREVIOUS_RDS=true
        fi
        if [ -z "$MSK_PASSWORD" ]; then
            USE_PREVIOUS_MSK=true
        fi
        return 0
    fi

    # New stack — passwords are required
    if [ -z "$RDS_PASSWORD" ]; then
        echo -n "Enter RDS PostgreSQL password (min 8 chars): "
        read -rs RDS_PASSWORD
        echo ""
        if [ ${#RDS_PASSWORD} -lt 8 ]; then
            log_error "RDS password must be at least 8 characters"
            exit 1
        fi
    fi

    if [ -z "$MSK_PASSWORD" ]; then
        echo -n "Enter MSK SASL/SCRAM password (min 8 chars): "
        read -rs MSK_PASSWORD
        echo ""
        if [ ${#MSK_PASSWORD} -lt 8 ]; then
            log_error "MSK password must be at least 8 characters"
            exit 1
        fi
    fi
}

# =============================================================================
# Validate Config
# =============================================================================
validate_config() {
    log_step "Validating configuration..."

    "$PYTHON_BIN" -m src.config_validator --config "$CONFIG_PATH" || {
        log_error "Config validation failed. Fix errors before deploying."
        exit 1
    }

    log_success "Config validation passed"
}

# =============================================================================
# Compile Config
# =============================================================================
compile_config() {
    log_step "Compiling configuration artifacts..."

    rm -rf "$BUILD_DIR"

    "$PYTHON_BIN" -m src.config_compiler --config "$CONFIG_PATH" --output "$BUILD_DIR" || {
        log_error "Config compilation failed."
        exit 1
    }

    log_success "Config compiled to ${BUILD_DIR}/"
}

# =============================================================================
# Utility Functions
# =============================================================================
stack_exists() {
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null
}

get_stack_output() {
    local output_key="$1"
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue" \
        --output text 2>/dev/null
}

wait_for_stack() {
    local operation="$1"
    local timeout=3600
    local interval=30
    local elapsed=0

    log_info "Waiting for stack ${operation}..."

    while [ $elapsed -lt $timeout ]; do
        local status
        status=$(stack_exists)

        case "$status" in
            CREATE_COMPLETE|UPDATE_COMPLETE)
                log_success "Stack ${operation} completed"
                return 0
                ;;
            CREATE_IN_PROGRESS|UPDATE_IN_PROGRESS)
                echo -ne "\r  Stack status: ${status} (${elapsed}s)    "
                sleep $interval
                elapsed=$((elapsed + interval))
                ;;
            CREATE_FAILED|UPDATE_FAILED|ROLLBACK_COMPLETE|ROLLBACK_FAILED)
                echo ""
                log_error "Stack ${operation} failed: ${status}"
                return 1
                ;;
            *)
                sleep $interval
                elapsed=$((elapsed + interval))
                ;;
        esac
    done

    echo ""
    log_error "Timeout waiting for stack"
    return 1
}

# =============================================================================
# Prerequisites Check
# =============================================================================
check_prerequisites() {
    log_step "Checking prerequisites..."

    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI not found. Please install it first."
        exit 1
    fi

    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured."
        exit 1
    fi

    local required_files=(
        "cloudformation/streaming-etl.yaml"
        "src/glue_streaming_job.py"
        "src/config_validator.py"
        "src/config_compiler.py"
    )

    for file in "${required_files[@]}"; do
        if [ ! -f "${PROJECT_ROOT}/${file}" ]; then
            log_error "Required file not found: ${file}"
            exit 1
        fi
    done

    local account_id
    account_id=$(aws sts get-caller-identity --query Account --output text)
    log_info "AWS Account: ${account_id}"
    log_info "Region: ${REGION}"
    # Ensure PyYAML is installed locally (needed for config validator and compiler)
    if ! "$PYTHON_BIN" -c "import yaml" 2>/dev/null; then
        log_info "Installing PyYAML for local config validation..."
        "$PYTHON_BIN" -m pip install "PyYAML==6.0.1" -q || {
            log_error "Failed to install PyYAML. Run: $PYTHON_BIN -m pip install PyYAML==6.0.1"
            exit 1
        }
    fi

    log_success "Prerequisites OK"
}

# =============================================================================
# Deploy CloudFormation Stack
# =============================================================================
deploy_cloudformation_stack() {
    log_step "Deploying CloudFormation stack..."

    local template_file="${PROJECT_ROOT}/cloudformation/streaming-etl.yaml"
    local stack_status

    # Auto-detect if dms-vpc-role already exists
    local create_dms_vpc_role="true"
    if aws iam get-role --role-name dms-vpc-role --region "$REGION" > /dev/null 2>&1; then
        log_info "dms-vpc-role already exists in this account, skipping creation"
        create_dms_vpc_role="false"
    fi

    # Build parameters JSON file (avoids shell escaping issues with inline JSON)
    local dms_mappings
    dms_mappings=$(cat "${BUILD_DIR}/dms_table_mappings.json")
    local params_file="/tmp/cfn-params.json"
    "$PYTHON_BIN" -c "
import json, sys
# Athena/Glue identifiers cannot contain hyphens, so derive a sanitized
# database name from the stack name (hyphens -> underscores) + _db suffix.
glue_db = sys.argv[1].replace('-', '_') + '_db'
params = [
    {'ParameterKey': 'EnvironmentName', 'ParameterValue': sys.argv[1]},
    {'ParameterKey': 'CreateDMSVPCRole', 'ParameterValue': sys.argv[2]},
    {'ParameterKey': 'DMSTableMappings', 'ParameterValue': sys.argv[3]},
    {'ParameterKey': 'GlueDatabaseName', 'ParameterValue': glue_db},
]
rds_pw = sys.argv[5]
msk_pw = sys.argv[6]
use_prev_rds = sys.argv[7]
use_prev_msk = sys.argv[8]
if use_prev_rds == 'true':
    params.append({'ParameterKey': 'RDSPassword', 'UsePreviousValue': True})
elif rds_pw:
    params.append({'ParameterKey': 'RDSPassword', 'ParameterValue': rds_pw})
if use_prev_msk == 'true':
    params.append({'ParameterKey': 'MSKPassword', 'UsePreviousValue': True})
elif msk_pw:
    params.append({'ParameterKey': 'MSKPassword', 'ParameterValue': msk_pw})
with open(sys.argv[4], 'w') as f:
    json.dump(params, f)
" "$STACK_NAME" "$create_dms_vpc_role" "$dms_mappings" "$params_file" \
  "$RDS_PASSWORD" "$MSK_PASSWORD" "${USE_PREVIOUS_RDS:-false}" "${USE_PREVIOUS_MSK:-false}"

    stack_status=$(stack_exists || echo "DOES_NOT_EXIST")

    # Upload template to S3 for large template support (>51200 bytes)
    # For new stacks, create a temp bucket; for existing stacks, use assets bucket
    local template_url=""
    if [ "$stack_status" = "DOES_NOT_EXIST" ]; then
        # Create stack using local template (must be under 51200 bytes for create)
        # If template is too large, create a temp S3 bucket first
        local template_size
        template_size=$(wc -c < "$template_file")
        if [ "$template_size" -gt 51200 ]; then
            local temp_bucket="${STACK_NAME}-cfn-temp-${RANDOM}"
            log_info "Template exceeds 51200 bytes, uploading to temporary S3 bucket..."
            aws s3 mb "s3://${temp_bucket}" --region "$REGION" > /dev/null 2>&1
            aws s3 cp "$template_file" "s3://${temp_bucket}/template.yaml" --region "$REGION" > /dev/null 2>&1
            template_url="https://${temp_bucket}.s3.${REGION}.amazonaws.com/template.yaml"
        fi

        log_info "Creating new stack: ${STACK_NAME}"

        if [ -n "$template_url" ]; then
            aws cloudformation create-stack \
                --stack-name "$STACK_NAME" \
                --template-url "$template_url" \
                --region "$REGION" \
                --capabilities CAPABILITY_NAMED_IAM \
                --parameters "file://${params_file}" \
                --tags Key=Project,Value=streaming-etl \
                --output text > /tmp/stack-output.txt 2>&1 || {
                    log_error "Failed to create stack"
                    cat /tmp/stack-output.txt
                    aws s3 rb "s3://${temp_bucket}" --force --region "$REGION" > /dev/null 2>&1
                    exit 1
                }
        else
            aws cloudformation create-stack \
                --stack-name "$STACK_NAME" \
                --template-body "file://${template_file}" \
                --region "$REGION" \
                --capabilities CAPABILITY_NAMED_IAM \
                --parameters "file://${params_file}" \
                --tags Key=Project,Value=streaming-etl \
                --output text > /tmp/stack-output.txt 2>&1 || {
                    log_error "Failed to create stack"
                    cat /tmp/stack-output.txt
                    exit 1
                }
        fi

        wait_for_stack "creation"

        # Clean up temp bucket AFTER stack creation completes (CFn needs template accessible during creation)
        if [ -n "$template_url" ]; then
            aws s3 rb "s3://${temp_bucket}" --force --region "$REGION" > /dev/null 2>&1 || true
            log_info "Cleaned up temporary template bucket"
        fi

    elif [[ "$stack_status" == *"COMPLETE"* ]] && [[ "$stack_status" != *"ROLLBACK"* ]]; then
        log_info "Stack exists with status: ${stack_status}"
        log_info "Updating stack..."

        # Upload template to assets bucket for update (no size limit via S3)
        local assets_bucket
        assets_bucket=$(get_stack_output "AssetsBucket")
        if [ -n "$assets_bucket" ]; then
            aws s3 cp "$template_file" "s3://${assets_bucket}/cloudformation/template.yaml" --region "$REGION" > /dev/null 2>&1 || {
                log_error "Failed to upload template to assets bucket"
                exit 1
            }
            local update_template_url="https://${assets_bucket}.s3.${REGION}.amazonaws.com/cloudformation/template.yaml"
            aws cloudformation update-stack \
                --stack-name "$STACK_NAME" \
                --template-url "$update_template_url" \
                --region "$REGION" \
                --capabilities CAPABILITY_NAMED_IAM \
                --parameters "file://${params_file}" \
                --output text > /tmp/stack-output.txt 2>&1 || {
                    if grep -q "No updates" /tmp/stack-output.txt; then
                        log_info "No stack updates needed"
                    else
                        log_warn "Stack update issue - check /tmp/stack-output.txt"
                    fi
                }
        else
            aws cloudformation update-stack \
                --stack-name "$STACK_NAME" \
                --template-body "file://${template_file}" \
                --region "$REGION" \
                --capabilities CAPABILITY_NAMED_IAM \
                --parameters "file://${params_file}" \
                --output text > /tmp/stack-output.txt 2>&1 || {
                    if grep -q "No updates" /tmp/stack-output.txt; then
                        log_info "No stack updates needed"
                    else
                        log_warn "Stack update issue - check /tmp/stack-output.txt"
                    fi
                }
        fi

        local current_status
        current_status=$(stack_exists)
        if [ "$current_status" = "UPDATE_IN_PROGRESS" ]; then
            wait_for_stack "update"
        fi
    else
        log_error "Stack in invalid state: ${stack_status}"
        exit 1
    fi

    log_success "CloudFormation stack ready"
}

# =============================================================================
# Upload Assets to S3
# =============================================================================
upload_assets_to_s3() {
    log_step "Uploading assets to S3..."

    local assets_bucket
    assets_bucket=$(get_stack_output "AssetsBucket")

    if [ -z "$assets_bucket" ]; then
        log_error "Could not get AssetsBucket from stack outputs"
        exit 1
    fi

    log_info "Assets bucket: ${assets_bucket}"

    # Upload Glue script
    log_info "Uploading Glue streaming script..."
    aws s3 cp "${PROJECT_ROOT}/src/glue_streaming_job.py" \
        "s3://${assets_bucket}/scripts/" --region "$REGION"

    # Upload native Glue DQ analyzer + DQDL compiler (referenced by --extra-py-files)
    log_info "Uploading Glue DQ analyzer and DQDL compiler..."
    aws s3 cp "${PROJECT_ROOT}/src/glue_dq_analyzer.py" \
        "s3://${assets_bucket}/scripts/" --region "$REGION"
    aws s3 cp "${PROJECT_ROOT}/src/dqdl_compiler.py" \
        "s3://${assets_bucket}/scripts/" --region "$REGION"

    # Upload use-case config (to the standard config/ path that Lambdas expect)
    log_info "Uploading use-case configuration..."
    aws s3 cp "$CONFIG_PATH" "s3://${assets_bucket}/config/tables.yaml" --region "$REGION"

    # Upload compiled artifacts
    log_info "Uploading compiled artifacts..."
    aws s3 cp "${BUILD_DIR}/dms_table_mappings.json" "s3://${assets_bucket}/config/" --region "$REGION"
    aws s3 cp "${BUILD_DIR}/create_tables.sql" "s3://${assets_bucket}/config/" --region "$REGION"
    aws s3 cp "${BUILD_DIR}/cfn_parameters.json" "s3://${assets_bucket}/config/" --region "$REGION"

    # Upload use-case data generator
    if [ -f "$GENERATOR_PATH" ]; then
        log_info "Uploading data generator for ${USE_CASE}..."
        aws s3 cp "$GENERATOR_PATH" "s3://${assets_bucket}/scripts/data_generator.py" --region "$REGION"
    fi

    # Upload framework support modules
    log_info "Uploading config validator..."
    aws s3 cp "${PROJECT_ROOT}/src/config_validator.py" \
        "s3://${assets_bucket}/scripts/" --region "$REGION"
    log_info "Uploading config compiler..."
    aws s3 cp "${PROJECT_ROOT}/src/config_compiler.py" \
        "s3://${assets_bucket}/scripts/" --region "$REGION"

    # NOTE: no PyYAML wheel or Deequ JAR uploads for the Glue 6.0 job.
    # Native Glue DQ (awsgluedq) ships with the runtime, and Python deps
    # (boto3, PyYAML) are pip-installed at job start via
    # --additional-python-modules (see the template), which resolves the right
    # artifacts for whichever interpreter the Glue runner uses.
    # Lambda layers below are built from pinned PyPI versions at deploy time —
    # no binaries are committed to this repository (opaque binaries cannot be
    # code-reviewed, so they are never checked in).

    # Create and upload psycopg2 Lambda layer (REQUIRED for SQL Runner Lambda)
    log_info "Creating psycopg2 Lambda layer (pip install from PyPI)..."

    # Create layer directory structure
    local layer_dir="/tmp/psycopg2-layer"
    rm -rf "$layer_dir"
    mkdir -p "$layer_dir/python"

    # Install pinned version targeting the Lambda runtime (python3.10, x86_64)
    "$PYTHON_BIN" -m pip install "psycopg2-binary==2.9.9" \
        --target "$layer_dir/python" \
        --platform manylinux2014_x86_64 \
        --python-version 3.10 \
        --implementation cp \
        --only-binary=:all: -q || {
            log_error "Failed to pip install psycopg2-binary==2.9.9 for the Lambda layer"
            exit 1
        }

    # Create layer zip
    local layer_zip="/tmp/psycopg2-layer.zip"
    rm -f "$layer_zip"
    (cd "$layer_dir" && zip -rq "$layer_zip" python)

    # Upload layer zip to S3
    log_info "Uploading psycopg2 layer to S3..."
    aws s3 cp "$layer_zip" "s3://${assets_bucket}/layers/psycopg2-layer.zip" --region "$REGION"

    # Create Lambda Layer from S3
    log_info "Publishing psycopg2 Lambda layer..."
    local psycopg2_layer_arn
    psycopg2_layer_arn=$(aws lambda publish-layer-version \
        --layer-name "${STACK_NAME}-psycopg2" \
        --description "psycopg2-binary 2.9.9 for Python 3.10" \
        --content "S3Bucket=${assets_bucket},S3Key=layers/psycopg2-layer.zip" \
        --compatible-runtimes python3.10 \
        --compatible-architectures x86_64 \
        --region "$REGION" \
        --query 'LayerVersionArn' \
        --output text 2>/dev/null) || {
            log_warn "Failed to publish psycopg2 layer"
        }

    if [ -n "$psycopg2_layer_arn" ]; then
        log_info "psycopg2 Layer ARN: ${psycopg2_layer_arn}"

        # Attach layer to SQL Runner Lambda
        log_info "Attaching psycopg2 layer to SQL Runner Lambda..."
        aws lambda update-function-configuration \
            --function-name "${STACK_NAME}-sql-runner" \
            --layers "$psycopg2_layer_arn" \
            --region "$REGION" > /tmp/lambda-update.txt 2>&1 || {
                log_warn "Failed to attach psycopg2 layer to Lambda"
            }

        # Attach layer to Data Generator Lambda + deploy actual generator code
        if [ -f "$GENERATOR_PATH" ]; then
            log_info "Deploying data generator code to Lambda..."
            local gen_zip_dir="/tmp/data-generator-pkg"
            local gen_zip="/tmp/data-generator.zip"
            rm -rf "$gen_zip_dir" "$gen_zip"
            mkdir -p "$gen_zip_dir"
            cp "$GENERATOR_PATH" "$gen_zip_dir/data_generator.py"
            # Create index.py wrapper that delegates to data_generator.lambda_handler
            cat > "$gen_zip_dir/index.py" << 'GENEOF'
from data_generator import lambda_handler
GENEOF
            (cd "$gen_zip_dir" && zip -rq "$gen_zip" .)

            log_info "Attaching psycopg2 layer to Data Generator Lambda..."
            aws lambda update-function-configuration \
                --function-name "${STACK_NAME}-data-generator" \
                --layers "$psycopg2_layer_arn" \
                --region "$REGION" > /tmp/lambda-update.txt 2>&1 || {
                    log_warn "Failed to attach psycopg2 layer to Data Generator Lambda"
                }

            # Wait for config update to complete before updating code
            log_info "Waiting for Lambda config update..."
            aws lambda wait function-updated \
                --function-name "${STACK_NAME}-data-generator" \
                --region "$REGION" 2>/dev/null || sleep 10

            log_info "Updating Data Generator Lambda code..."
            aws lambda update-function-code \
                --function-name "${STACK_NAME}-data-generator" \
                --zip-file "fileb://${gen_zip}" \
                --region "$REGION" > /tmp/lambda-update.txt 2>&1 || {
                    log_warn "Failed to update Data Generator Lambda code"
                }

            rm -rf "$gen_zip_dir" "$gen_zip"
            log_success "Data Generator Lambda deployed with ${USE_CASE} generator"
        fi
    fi

    # Cleanup psycopg2 layer temp files
    rm -rf "$layer_dir" "$layer_zip"

    # Create and upload kafka-python Lambda layer (REQUIRED for Kafka Admin Lambda)
    log_info "Creating kafka-python Lambda layer (pip install from PyPI)..."

    # Create layer directory structure
    local kafka_layer_dir="/tmp/kafka-layer"
    rm -rf "$kafka_layer_dir"
    mkdir -p "$kafka_layer_dir/python"

    # Install pinned versions; PyYAML included for sync_from_config functionality
    "$PYTHON_BIN" -m pip install "kafka-python==2.3.0" "PyYAML==6.0.1" \
        --target "$kafka_layer_dir/python" \
        --platform manylinux2014_x86_64 \
        --python-version 3.10 \
        --implementation cp \
        --only-binary=:all: -q || {
            log_error "Failed to pip install kafka-python==2.3.0 / PyYAML==6.0.1 for the Lambda layer"
            exit 1
        }

    # Create layer zip
    local kafka_layer_zip="/tmp/kafka-layer.zip"
    rm -f "$kafka_layer_zip"
    (cd "$kafka_layer_dir" && zip -rq "$kafka_layer_zip" python)

    # Upload layer zip to S3
    log_info "Uploading kafka-python layer to S3..."
    aws s3 cp "$kafka_layer_zip" "s3://${assets_bucket}/layers/kafka-layer.zip" --region "$REGION"

    # Create Lambda Layer from S3
    log_info "Publishing kafka-python Lambda layer..."
    local kafka_layer_arn
    kafka_layer_arn=$(aws lambda publish-layer-version \
        --layer-name "${STACK_NAME}-kafka-python" \
        --description "kafka-python 2.3.0" \
        --content "S3Bucket=${assets_bucket},S3Key=layers/kafka-layer.zip" \
        --compatible-runtimes python3.10 python3.11 \
        --compatible-architectures x86_64 \
        --region "$REGION" \
        --query 'LayerVersionArn' \
        --output text 2>/dev/null) || {
            log_warn "Failed to publish kafka-python layer"
        }

    if [ -n "$kafka_layer_arn" ]; then
        log_info "kafka-python Layer ARN: ${kafka_layer_arn}"

        # Attach layer to Kafka Admin Lambda
        log_info "Attaching kafka-python layer to Kafka Admin Lambda..."
        aws lambda update-function-configuration \
            --function-name "${STACK_NAME}-kafka-admin" \
            --layers "$kafka_layer_arn" \
            --region "$REGION" > /tmp/lambda-update.txt 2>&1 || {
                log_warn "Failed to attach kafka-python layer to Lambda"
            }
    fi

    # Cleanup kafka layer temp files
    rm -rf "$kafka_layer_dir" "$kafka_layer_zip"

    # Create and upload PyYAML Lambda layer for Athena Table Creator
    log_info "Creating PyYAML Lambda layer for Athena Table Creator (pip install from PyPI)..."
    local pyyaml_layer_dir="/tmp/pyyaml-layer"
    rm -rf "$pyyaml_layer_dir"
    mkdir -p "$pyyaml_layer_dir/python"

    if "$PYTHON_BIN" -m pip install "PyYAML==6.0.1" \
        --target "$pyyaml_layer_dir/python" \
        --platform manylinux2014_x86_64 \
        --python-version 3.10 \
        --implementation cp \
        --only-binary=:all: -q; then
        local pyyaml_layer_zip="/tmp/pyyaml-layer.zip"
        rm -f "$pyyaml_layer_zip"
        (cd "$pyyaml_layer_dir" && zip -rq "$pyyaml_layer_zip" python)

        aws s3 cp "$pyyaml_layer_zip" "s3://${assets_bucket}/layers/pyyaml-layer.zip" --region "$REGION"

        log_info "Publishing PyYAML Lambda layer..."
        local pyyaml_layer_arn
        pyyaml_layer_arn=$(aws lambda publish-layer-version \
            --layer-name "${STACK_NAME}-pyyaml" \
            --description "PyYAML 6.0.1 for Python 3.10" \
            --content "S3Bucket=${assets_bucket},S3Key=layers/pyyaml-layer.zip" \
            --compatible-runtimes python3.10 python3.11 \
            --compatible-architectures x86_64 \
            --region "$REGION" \
            --query 'LayerVersionArn' \
            --output text 2>/dev/null) || {
                log_warn "Failed to publish PyYAML layer"
            }

        if [ -n "$pyyaml_layer_arn" ]; then
            log_info "PyYAML Layer ARN: ${pyyaml_layer_arn}"

            log_info "Attaching PyYAML layer to Athena Table Creator Lambda..."
            aws lambda update-function-configuration \
                --function-name "${STACK_NAME}-athena-table-creator" \
                --layers "$pyyaml_layer_arn" \
                --region "$REGION" > /tmp/lambda-update.txt 2>&1 || {
                    log_warn "Failed to attach PyYAML layer to Athena Lambda"
                }
        fi

        rm -rf "$pyyaml_layer_dir" "$pyyaml_layer_zip"
    else
        log_warn "Failed to pip install PyYAML==6.0.1 for the Athena layer"
    fi

    log_success "Assets uploaded (Glue DQ analyzer, psycopg2, kafka-python, and PyYAML layers)"
}

# =============================================================================
# Start DMS Replication
# =============================================================================
start_dms_replication() {
    log_step "Starting DMS replication task..."

    local dms_task_arn
    dms_task_arn=$(get_stack_output "DMSTaskArn")

    if [ -z "$dms_task_arn" ]; then
        log_warn "DMS task ARN not found"
        return 0
    fi

    local task_status
    task_status=$(aws dms describe-replication-tasks \
        --filters Name=replication-task-arn,Values="$dms_task_arn" \
        --region "$REGION" \
        --query 'ReplicationTasks[0].Status' \
        --output text 2>/dev/null || echo "unknown")

    log_info "DMS task status: ${task_status}"

    if [ "$task_status" = "stopped" ] || [ "$task_status" = "ready" ]; then
        log_info "Starting DMS task..."
        aws dms start-replication-task \
            --replication-task-arn "$dms_task_arn" \
            --start-replication-task-type start-replication \
            --region "$REGION" > /tmp/dms-output.txt 2>&1 || {
                log_warn "Failed to start DMS task"
            }
    fi

    log_success "DMS replication started"
}

# =============================================================================
# Start Glue Job (Optional)
# =============================================================================
start_glue_job() {
    if [ "$START_GLUE" = false ]; then
        log_info "Skipping Glue job start (use --start-glue to enable)"
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
        log_info "Waiting 30s for job to stop..."
        sleep 30
    fi

    log_info "Starting Glue job: ${glue_job_name}"
    aws glue start-job-run \
        --job-name "$glue_job_name" \
        --region "$REGION" > /tmp/glue-output.txt 2>&1 || {
            log_warn "Failed to start Glue job"
        }

    log_success "Glue job started"
}

# =============================================================================
# Display Summary
# =============================================================================
display_summary() {
    log_step "Deployment Summary"
    echo ""
    echo "Stack Name: ${STACK_NAME}"
    echo "Use Case:   ${USE_CASE}"
    echo "Region:     ${REGION}"
    echo "Config:     examples/${USE_CASE}/config/tables.yaml"
    echo ""

    local rds_endpoint delta_bucket assets_bucket glue_job
    rds_endpoint=$(get_stack_output "RDSEndpoint")
    delta_bucket=$(get_stack_output "DeltaBucket")
    assets_bucket=$(get_stack_output "AssetsBucket")
    glue_job=$(get_stack_output "GlueJobName")

    [ -n "$rds_endpoint" ] && echo "RDS Endpoint: ${rds_endpoint}"
    [ -n "$delta_bucket" ] && echo "Delta Bucket: ${delta_bucket}"
    [ -n "$assets_bucket" ] && echo "Assets Bucket: ${assets_bucket}"
    [ -n "$glue_job" ] && echo "Glue Job: ${glue_job}"
    echo ""

    echo "Next Steps:"
    echo "  1. Run post-deploy setup: ./scripts/post-deploy.sh --stack-name ${STACK_NAME} --use-case ${USE_CASE}"
    echo "  2. Generate test data: aws lambda invoke --function-name ${STACK_NAME}-data-generator ..."
    echo "  3. Query data in Athena"
    echo ""
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo -e "${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║     Streaming ETL Framework - One-Click Deployment         ║${NC}"
    echo -e "${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    parse_args "$@"

    validate_use_case

    log_info "Stack Name: ${STACK_NAME}"
    log_info "Use Case: ${USE_CASE}"
    log_info "Region: ${REGION}"
    echo ""

    check_prerequisites
    echo ""

    prompt_passwords
    echo ""

    validate_config
    echo ""

    compile_config
    echo ""

    if [ "$SKIP_STACK" = false ]; then
        deploy_cloudformation_stack
        echo ""
    fi

    upload_assets_to_s3
    echo ""

    # DMS is started by post-deploy.sh after RDS tables are created
    # Starting it here would fail with "no tables found"

    start_glue_job
    echo ""

    display_summary

    log_success "Deployment complete!"
    echo ""
}

main "$@"
