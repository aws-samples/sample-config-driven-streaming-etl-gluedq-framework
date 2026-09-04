#!/bin/bash
# =============================================================================
# EC2 Setup Script for Amazon Linux 2023
# =============================================================================
# Installs: system build tools, Python 3.10, AWS CLI v2, PyYAML
# Prerequisites: git must be installed first
# Usage: ./scripts/setup-ec2.sh
# =============================================================================

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

RED='\033[0;31m'

log_info()    { echo -e "${GREEN}[INFO]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}    $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC}   $1"; }
log_step()    { echo -e "${BOLD}[STEP]${NC}    $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}      $1"; }

echo ""
echo -e "${BOLD}EC2 Setup — Streaming ETL Framework${NC}"
echo ""

# Step 1: System dependencies
# NOTE: Amazon Linux 2023 does NOT ship git by default, so install it here
# (the repo is cloned with git before running this script; installing it also
# makes a clone-from-script flow work and lets the version print below succeed).
log_step "Installing system dependencies..."
sudo dnf install -y git curl unzip gcc openssl-devel bzip2-devel libffi-devel zlib-devel --allowerasing -q > /dev/null 2>&1
log_success "System dependencies"

# Step 2: Python 3.10
if command -v python3.10 &> /dev/null; then
    log_success "Python 3.10 (already installed)"
else
    # NOTE: this is the DEPLOY-HOST Python (used for the config validator/
    # compiler and deploy tooling only). The Glue 6.0 job itself runs Python
    # 3.13 on the Glue-managed runtime — nothing to install here for that.
    log_step "Installing Python 3.10 (deploy-host tooling, compiling from source, ~2 min)..."
    curl -sO https://www.python.org/ftp/python/3.10.14/Python-3.10.14.tgz
    # Verify checksum (SHA256 from python.org). Treat a missing sha256sum tool
    # or a checksum mismatch as a HARD failure - never compile/install an
    # unverified tarball (supply-chain safety).
    if ! command -v sha256sum &> /dev/null; then
        log_error "sha256sum not available - cannot verify Python tarball integrity. Aborting."
        exit 1
    fi
    # Official python.org SHA-256 for Python-3.10.14.tgz (MD5 f67d78c8323a18fe6b945914c51a7aa6).
    echo "cefea32d3be89c02436711c95a45c7f8e880105514b78680c14fe76f5709a0f6  Python-3.10.14.tgz" | sha256sum -c --quiet || {
        log_error "Python tarball checksum verification FAILED. Aborting to avoid installing a tampered build."
        rm -f Python-3.10.14.tgz
        exit 1
    }
    tar xzf Python-3.10.14.tgz
    cd Python-3.10.14
    ./configure > /dev/null 2>&1
    make -j$(nproc) > /dev/null 2>&1
    sudo make altinstall > /dev/null 2>&1
    cd ..
    sudo rm -rf Python-3.10.14 Python-3.10.14.tgz
    log_success "Python 3.10 installed"
fi

# Step 3: AWS CLI v2
if command -v aws &> /dev/null; then
    log_success "AWS CLI (already installed)"
else
    log_step "Installing AWS CLI v2..."
    curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install > /dev/null 2>&1
    rm -rf aws awscliv2.zip
    log_success "AWS CLI installed"
fi

# Step 4: PyYAML
python3.10 -m pip install "PyYAML==6.0.1" -q > /dev/null 2>&1 || \
    python3.10 -m pip install "PyYAML==6.0.1" --user -q > /dev/null 2>&1
log_success "PyYAML"

echo ""
log_step "Installed versions:"
echo "  git:        $(git --version 2>&1 | head -1)"
echo "  python3.10: $(python3.10 --version 2>&1)"
echo "  aws:        $(aws --version 2>&1 | head -1)"
echo "  PyYAML:     $(python3.10 -c 'import yaml; print(yaml.__version__)' 2>&1)"
echo ""
log_success "Setup complete. Configure AWS credentials, then run deploy.sh"
echo ""
