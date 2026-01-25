#!/bin/bash
# =============================================================================
# KV-Cache AWS Configuration
# =============================================================================

# -----------------------------------------------------------------------------
# REQUIRED: Your Docker Hub Image
# -----------------------------------------------------------------------------
DOCKER_IMAGE="sushant9473/kv-cache:latest"

# -----------------------------------------------------------------------------
# AWS Configuration
# -----------------------------------------------------------------------------

# AWS Region
AWS_REGION="ap-south-1"

# EC2 Instance Type
INSTANCE_TYPE="t3.small"

# Key Pair Name (for SSH access)
KEY_NAME="kv-cache-key"

# Security Group Name
SECURITY_GROUP="kv-cache-sg"

# -----------------------------------------------------------------------------
# Load Test Configuration
# -----------------------------------------------------------------------------

# Number of concurrent connections
LOAD_TEST_CONNECTIONS=100

# Number of requests per connection
LOAD_TEST_REQUESTS=1000

# PUT to GET ratio (0.5 = 50% PUT, 50% GET)
LOAD_TEST_RATIO=0.5

# TTL for PUT operations (0 = no TTL)
LOAD_TEST_TTL=0

# -----------------------------------------------------------------------------
# Advanced Configuration
# -----------------------------------------------------------------------------

# AMI ID (Amazon Linux 2023)
AMI_ID=""

# Instance tags
PROJECT_TAG="kv-cache"

# SSH connection timeout (seconds)
SSH_TIMEOUT=30

# Max wait time for instance startup (seconds)
INSTANCE_WAIT_TIMEOUT=300
