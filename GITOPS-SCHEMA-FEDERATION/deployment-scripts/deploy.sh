#!/bin/bash

###############################################################################
# GitOps Schema Federation Manager - Deployment Script
#
# This script deploys schemas from SCHEMASTORE to configured Schema Registries
# Usage: ./deploy.sh [OPTIONS]
###############################################################################

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CONFIG_FILE="deployment-config.yaml"
SCHEMA_STORE="SCHEMASTORE"
DRY_RUN=false
VERBOSE=false
FILTER_SUBJECT=""
FILTER_REGION=""
PARALLEL_JOBS=3

# Counters
TOTAL_DEPLOYMENTS=0
SUCCESSFUL_DEPLOYMENTS=0
FAILED_DEPLOYMENTS=0

###############################################################################
# Helper Functions
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -c, --config FILE       Path to deployment config file (default: deployment-config.yaml)
    -s, --schema-store DIR  Path to SCHEMASTORE directory (default: SCHEMASTORE)
    -d, --dry-run          Simulate deployment without making changes
    -v, --verbose          Enable verbose logging
    --subject PATTERN      Deploy only subjects matching pattern
    --region NAME          Deploy only to specified region
    -j, --jobs N           Number of parallel jobs (default: 3)
    -h, --help             Show this help message

Examples:
    # Deploy all schemas to all regions
    $0

    # Dry run deployment
    $0 --dry-run

    # Deploy specific subject to specific region
    $0 --subject user-events-value --region us-west-local

    # Deploy with custom config
    $0 --config prod-deployment-config.yaml

EOF
}

check_dependencies() {
    local missing_deps=0

    for cmd in jq yq curl; do
        if ! command -v $cmd &> /dev/null; then
            log_error "Required command not found: $cmd"
            missing_deps=$((missing_deps + 1))
        fi
    done

    if [ $missing_deps -gt 0 ]; then
        log_error "Please install missing dependencies and try again"
        exit 1
    fi
}

load_config() {
    if [ ! -f "$CONFIG_FILE" ]; then
        log_error "Config file not found: $CONFIG_FILE"
        exit 1
    fi

    log_info "Loading configuration from: $CONFIG_FILE"

    # Parse YAML and convert to JSON (requires yq)
    CONFIG_JSON=$(yq eval -o=json "$CONFIG_FILE")
}

get_registry_url() {
    local region=$1
    echo "$CONFIG_JSON" | jq -r ".regions.\"$region\".schemaRegistryUrl"
}

get_auth_type() {
    local region=$1
    echo "$CONFIG_JSON" | jq -r ".regions.\"$region\".authType // \"none\""
}

get_auth_credentials() {
    local region=$1
    local username=$(echo "$CONFIG_JSON" | jq -r ".regions.\"$region\".username // \"\"")
    local password=$(echo "$CONFIG_JSON" | jq -r ".regions.\"$region\".password // \"\"")

    # Expand environment variables
    username=$(eval echo "$username")
    password=$(eval echo "$password")

    echo "$username:$password"
}

test_sr_connection() {
    local region=$1
    local sr_url=$(get_registry_url "$region")
    local auth_type=$(get_auth_type "$region")

    log_info "Testing connection to $region ($sr_url)..."

    local curl_cmd="curl -s -w \"\n%{http_code}\" "

    if [ "$auth_type" == "basic" ]; then
        local creds=$(get_auth_credentials "$region")
        curl_cmd="$curl_cmd -u \"$creds\" "
    fi

    curl_cmd="$curl_cmd \"$sr_url/schemas/types\""

    local response=$(eval $curl_cmd)
    local http_code=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | head -n-1)

    if [ "$http_code" == "200" ]; then
        log_success "Connection successful to $region"
        return 0
    else
        log_error "Connection failed to $region (HTTP $http_code)"
        [ "$VERBOSE" == "true" ] && echo "$body"
        return 1
    fi
}

check_sr_mode() {
    local region=$1
    local sr_url=$(get_registry_url "$region")
    local auth_type=$(get_auth_type "$region")

    local curl_cmd="curl -s "

    if [ "$auth_type" == "basic" ]; then
        local creds=$(get_auth_credentials "$region")
        curl_cmd="$curl_cmd -u \"$creds\" "
    fi

    curl_cmd="$curl_cmd \"$sr_url/mode\""

    local response=$(eval $curl_cmd)
    local mode=$(echo "$response" | jq -r '.mode')

    if [ "$mode" == "IMPORT" ]; then
        log_success "Schema Registry $region is in IMPORT mode"
        return 0
    else
        log_warn "Schema Registry $region is in $mode mode (expected IMPORT)"
        return 1
    fi
}

deploy_schema_to_region() {
    local schema_path=$1
    local region=$2
    local subject=$3
    local schema_id=$4
    local schema_type=$5

    TOTAL_DEPLOYMENTS=$((TOTAL_DEPLOYMENTS + 1))

    local sr_url=$(get_registry_url "$region")
    local auth_type=$(get_auth_type "$region")

    # Read schema content
    local schema_file="$schema_path/schema.avsc"
    if [ ! -f "$schema_file" ]; then
        schema_file="$schema_path/schema.proto"
    fi
    if [ ! -f "$schema_file" ]; then
        schema_file="$schema_path/schema.json"
    fi

    if [ ! -f "$schema_file" ]; then
        log_error "Schema file not found in $schema_path"
        FAILED_DEPLOYMENTS=$((FAILED_DEPLOYMENTS + 1))
        return 1
    fi

    # Read and escape schema content for JSON
    local schema_content=$(cat "$schema_file" | jq -Rs .)

    # Build request payload
    local payload=$(jq -n \
        --arg schema "$schema_content" \
        --arg schemaType "$schema_type" \
        --argjson id "$schema_id" \
        '{schema: ($schema | fromjson | tojson), schemaType: $schemaType, id: $id}')

    if [ "$DRY_RUN" == "true" ]; then
        log_info "[DRY-RUN] Would deploy $subject (ID: $schema_id) to $region"
        SUCCESSFUL_DEPLOYMENTS=$((SUCCESSFUL_DEPLOYMENTS + 1))
        return 0
    fi

    # Build curl command
    local curl_cmd="curl -s -w \"\n%{http_code}\" -X POST "

    if [ "$auth_type" == "basic" ]; then
        local creds=$(get_auth_credentials "$region")
        curl_cmd="$curl_cmd -u \"$creds\" "
    fi

    curl_cmd="$curl_cmd -H \"Content-Type: application/json\" "
    curl_cmd="$curl_cmd -d '$payload' "
    curl_cmd="$curl_cmd \"$sr_url/subjects/$subject/versions\""

    [ "$VERBOSE" == "true" ] && log_info "Executing: $curl_cmd"

    # Execute deployment
    local response=$(eval $curl_cmd)
    local http_code=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | head -n-1)

    if [ "$http_code" == "200" ] || [ "$http_code" == "201" ]; then
        local returned_id=$(echo "$body" | jq -r '.id')

        if [ "$returned_id" == "$schema_id" ]; then
            log_success "Deployed $subject (ID: $schema_id) to $region"
            SUCCESSFUL_DEPLOYMENTS=$((SUCCESSFUL_DEPLOYMENTS + 1))
            return 0
        else
            log_error "ID mismatch for $subject in $region (expected: $schema_id, got: $returned_id)"
            FAILED_DEPLOYMENTS=$((FAILED_DEPLOYMENTS + 1))
            return 1
        fi
    else
        log_error "Failed to deploy $subject to $region (HTTP $http_code)"
        [ "$VERBOSE" == "true" ] && echo "$body"
        FAILED_DEPLOYMENTS=$((FAILED_DEPLOYMENTS + 1))
        return 1
    fi
}

find_all_schemas() {
    if [ ! -d "$SCHEMA_STORE" ]; then
        log_error "Schema store directory not found: $SCHEMA_STORE"
        exit 1
    fi

    log_info "Scanning $SCHEMA_STORE for schemas..."

    local schemas=()

    # Find all metadata.json files
    while IFS= read -r -d '' metadata_file; do
        local schema_dir=$(dirname "$metadata_file")

        # Extract context, subject, version from path
        # Path format: SCHEMASTORE/{context}/{subject}/v{version}/metadata.json
        local rel_path="${metadata_file#$SCHEMA_STORE/}"
        local context=$(echo "$rel_path" | cut -d'/' -f1)
        local subject=$(echo "$rel_path" | cut -d'/' -f2)
        local version_dir=$(echo "$rel_path" | cut -d'/' -f3)
        local version=$(echo "$version_dir" | sed 's/v//')

        # Apply subject filter if specified
        if [ -n "$FILTER_SUBJECT" ] && [[ ! "$subject" =~ $FILTER_SUBJECT ]]; then
            continue
        fi

        schemas+=("$schema_dir|$context|$subject|$version|$metadata_file")
    done < <(find "$SCHEMA_STORE" -name "metadata.json" -type f -print0)

    echo "${schemas[@]}"
}

deploy_all_schemas() {
    local schemas=($(find_all_schemas))

    if [ ${#schemas[@]} -eq 0 ]; then
        log_warn "No schemas found in $SCHEMA_STORE"
        return 0
    fi

    log_info "Found ${#schemas[@]} schema(s) to deploy"

    for schema_info in "${schemas[@]}"; do
        IFS='|' read -r schema_dir context subject version metadata_file <<< "$schema_info"

        # Load metadata
        local metadata=$(cat "$metadata_file")
        local schema_id=$(echo "$metadata" | jq -r '.schemaId')
        local schema_type=$(echo "$metadata" | jq -r '.schemaType')
        local deployment_regions=($(echo "$metadata" | jq -r '.deploymentRegions[]'))

        log_info "Processing: $subject v$version (ID: $schema_id)"

        # Deploy to each target region
        for region in "${deployment_regions[@]}"; do
            # Apply region filter if specified
            if [ -n "$FILTER_REGION" ] && [ "$region" != "$FILTER_REGION" ]; then
                continue
            fi

            deploy_schema_to_region "$schema_dir" "$region" "$subject" "$schema_id" "$schema_type"
        done
    done
}

print_summary() {
    echo
    echo "======================================================================"
    echo "                     Deployment Summary"
    echo "======================================================================"
    echo "Total deployments:      $TOTAL_DEPLOYMENTS"
    echo -e "Successful deployments: ${GREEN}$SUCCESSFUL_DEPLOYMENTS${NC}"
    echo -e "Failed deployments:     ${RED}$FAILED_DEPLOYMENTS${NC}"
    echo "======================================================================"
    echo

    if [ $FAILED_DEPLOYMENTS -gt 0 ]; then
        log_error "Some deployments failed. Check logs above for details."
        return 1
    else
        log_success "All deployments completed successfully!"
        return 0
    fi
}

###############################################################################
# Main Script
###############################################################################

main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            -s|--schema-store)
                SCHEMA_STORE="$2"
                shift 2
                ;;
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            --subject)
                FILTER_SUBJECT="$2"
                shift 2
                ;;
            --region)
                FILTER_REGION="$2"
                shift 2
                ;;
            -j|--jobs)
                PARALLEL_JOBS="$2"
                shift 2
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done

    echo "======================================================================"
    echo "      GitOps Schema Federation Manager - Deployment Script"
    echo "======================================================================"
    echo

    [ "$DRY_RUN" == "true" ] && log_warn "DRY RUN MODE - No changes will be made"

    # Check dependencies
    check_dependencies

    # Load configuration
    load_config

    # Test connections to all Schema Registries
    log_info "Testing Schema Registry connections..."
    local regions=($(echo "$CONFIG_JSON" | jq -r '.regions | keys[]'))

    for region in "${regions[@]}"; do
        # Apply region filter if specified
        if [ -n "$FILTER_REGION" ] && [ "$region" != "$FILTER_REGION" ]; then
            continue
        fi

        if ! test_sr_connection "$region"; then
            log_error "Cannot proceed - connection test failed for $region"
            exit 1
        fi

        if ! check_sr_mode "$region"; then
            log_warn "Schema Registry $region may not accept custom IDs"
        fi
    done

    echo
    log_info "Starting schema deployment..."
    echo

    # Deploy all schemas
    deploy_all_schemas

    # Print summary
    print_summary
}

# Run main function
main "$@"
