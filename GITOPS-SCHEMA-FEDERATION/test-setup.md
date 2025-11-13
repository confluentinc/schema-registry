# GitOps Schema Federation Manager - Test Setup Guide

**Version:** 1.0
**Date:** 2025-11-13
**Purpose:** Step-by-step guide to set up the test environment

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Phase 1: Single Schema Registry](#phase-1-single-schema-registry)
3. [Phase 2: Multi-Region Setup](#phase-2-multi-region-setup)
4. [Phase 3: Confluent Cloud Integration](#phase-3-confluent-cloud-integration)
5. [Testing Procedures](#testing-procedures)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software

```bash
# Check versions
docker --version          # >= 20.10
docker-compose --version  # >= 2.0
git --version             # >= 2.30
python3 --version         # >= 3.10
node --version            # >= 18.0
npm --version             # >= 9.0
```

### Install Dependencies

```bash
# Clone repository
git clone https://github.com/your-org/global-schema-registry.git
cd global-schema-registry/GITOPS-SCHEMA-FEDERATION

# Install Python dependencies (API service)
cd api-service
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Install Node dependencies (Web UI)
cd ../web-ui
npm install

# Return to root
cd ..
```

---

## Phase 1: Single Schema Registry

**Goal:** Start with minimal setup - one Schema Registry for basic testing

### Step 1: Create Minimal Docker Compose

**File:** `docker-compose-phase1.yml`

```yaml
version: '3.8'

services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka
    container_name: kafka-test
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    hostname: schema-registry
    container_name: schema-registry-test
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_MODE_MUTABILITY: "true"

  control-center:
    image: confluentinc/cp-enterprise-control-center:7.6.0
    hostname: control-center
    container_name: control-center-test
    depends_on:
      - kafka
      - schema-registry
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: kafka:29092
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
      CONFLUENT_METRICS_TOPIC_REPLICATION: 1
      PORT: 9021

networks:
  default:
    name: schema-test-network
```

### Step 2: Start Infrastructure

```bash
# Start containers
docker-compose -f docker-compose-phase1.yml up -d

# Wait for services to be ready (30-60 seconds)
sleep 60

# Verify Kafka
docker logs kafka-test | grep "started (kafka.server.KafkaServer)"

# Verify Schema Registry
curl http://localhost:8081/schemas/types
# Expected: ["JSON","PROTOBUF","AVRO"]

# Verify Control Center
open http://localhost:9021
```

### Step 3: Configure IMPORT Mode

```bash
# Set Schema Registry to IMPORT mode
curl -X PUT http://localhost:8081/mode \
  -H "Content-Type: application/json" \
  --data '{"mode": "IMPORT"}'

# Verify mode
curl http://localhost:8081/mode
# Expected: {"mode":"IMPORT"}
```

### Step 4: Create Initial Schema Store Structure

```bash
# Create directory structure
mkdir -p SCHEMASTORE/default/user-events-value/v1
mkdir -p SCHEMASTORE/default/order-events-value/v1

# Initialize Git repository (if not already)
git init
git add SCHEMASTORE/
git commit -m "Initialize SCHEMASTORE"
```

### Step 5: Add Example Schema

**File:** `SCHEMASTORE/default/user-events-value/v1/schema.avsc`

```json
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.example.events",
  "fields": [
    {
      "name": "userId",
      "type": "string",
      "doc": "Unique user identifier"
    },
    {
      "name": "eventType",
      "type": "string",
      "doc": "Type of event (LOGIN, LOGOUT, SIGNUP, etc.)"
    },
    {
      "name": "timestamp",
      "type": "long",
      "logicalType": "timestamp-millis",
      "doc": "Event timestamp in milliseconds"
    }
  ]
}
```

**File:** `SCHEMASTORE/default/user-events-value/v1/metadata.json`

```json
{
  "schemaId": 10001,
  "subject": "user-events-value",
  "version": 1,
  "context": "default",
  "schemaType": "AVRO",
  "compatibilityMode": "BACKWARD",
  "deploymentRegions": ["local"],
  "createdAt": "2025-11-13T10:00:00Z",
  "createdBy": "test-user",
  "description": "User event schema for tracking user activities",
  "tags": ["events", "users"]
}
```

### Step 6: Create Deployment Config

**File:** `deployment-config-phase1.yaml`

```yaml
regions:
  local:
    schemaRegistryUrl: http://localhost:8081
    authType: none
    mode: IMPORT

deploymentRules:
  - context: default
    subjects: ["*"]
    regions: ["local"]
```

### Step 7: Test Manual Deployment

```bash
# Deploy schema manually using curl
SCHEMA_CONTENT=$(cat SCHEMASTORE/default/user-events-value/v1/schema.avsc | jq -c .)

curl -X POST http://localhost:8081/subjects/user-events-value/versions \
  -H "Content-Type: application/json" \
  --data "{
    \"schema\": $SCHEMA_CONTENT,
    \"schemaType\": \"AVRO\",
    \"id\": 10001
  }"

# Expected response: {"id":10001}

# Verify registration
curl http://localhost:8081/schemas/ids/10001 | jq .

# List all subjects
curl http://localhost:8081/subjects | jq .
```

### Step 8: Initialize ID Service Database

```bash
# Create database directory
mkdir -p data

# Initialize SQLite database
python3 << EOF
import sqlite3

conn = sqlite3.connect('data/schema_ids.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS schema_ids (
    id INTEGER PRIMARY KEY,
    schema_hash VARCHAR(64) UNIQUE NOT NULL,
    subject VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    context VARCHAR(255) DEFAULT 'default',
    schema_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    metadata TEXT,
    UNIQUE(context, subject, version)
)
''')

# Insert test record
cursor.execute('''
INSERT INTO schema_ids (id, schema_hash, subject, version, context, schema_type, created_by)
VALUES (10001, 'a1b2c3d4e5f6...', 'user-events-value', 1, 'default', 'AVRO', 'test-user')
''')

conn.commit()
conn.close()
print("Database initialized successfully")
EOF
```

### Step 9: Test API Service

```bash
# Start API service
cd api-service
source venv/bin/activate
uvicorn app.main:app --reload --port 8080

# In another terminal, test API
curl http://localhost:8080/health
# Expected: {"status":"healthy"}

# Test schema listing
curl http://localhost:8080/api/v1/schemas | jq .

# Test ID allocation
curl -X POST http://localhost:8080/api/v1/schema-ids/allocate \
  -H "Content-Type: application/json" \
  --data '{
    "subject": "order-events-value",
    "version": 1,
    "context": "default",
    "schemaType": "AVRO",
    "schemaContent": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}"
  }'

# Expected: {"id":10002,"existing":false}
```

### Step 10: Test Web UI

```bash
# Start Web UI development server
cd web-ui
npm run dev

# Open browser
open http://localhost:3000

# Expected: See dashboard with connection to API
```

### Phase 1 Success Criteria

- ✅ Kafka and Schema Registry running
- ✅ Schema Registry in IMPORT mode
- ✅ Example schema deployed with custom ID (10001)
- ✅ ID service database initialized
- ✅ API service responds to health checks
- ✅ Web UI loads and connects to API

---

## Phase 2: Multi-Region Setup

**Goal:** Add second Schema Registry to test multi-region deployment

### Step 1: Extend Docker Compose

**File:** `docker-compose-phase2.yml`

Includes all services from Phase 1 plus:

```yaml
  # Add to existing docker-compose-phase1.yml

  kafka-eu:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka-eu
    container_name: kafka-eu-test
    ports:
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-eu:29092,PLAINTEXT_HOST://localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 2
      KAFKA_CONTROLLER_QUORUM_VOTERS: 2@kafka-eu:29093
      KAFKA_LISTENERS: PLAINTEXT://kafka-eu:29092,CONTROLLER://kafka-eu:29093,PLAINTEXT_HOST://0.0.0.0:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      CLUSTER_ID: NlU4OEVBNTcwNTJENDM2Qk

  schema-registry-eu:
    image: confluentinc/cp-schema-registry:7.6.0
    hostname: schema-registry-eu
    container_name: schema-registry-eu-test
    depends_on:
      - kafka-eu
    ports:
      - "8082:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry-eu
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-eu:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_MODE_MUTABILITY: "true"
```

### Step 2: Start Multi-Region Infrastructure

```bash
# Stop Phase 1 containers
docker-compose -f docker-compose-phase1.yml down

# Start Phase 2 with both regions
docker-compose -f docker-compose-phase2.yml up -d

# Wait for startup
sleep 60

# Verify both SRs
curl http://localhost:8081/schemas/types  # US
curl http://localhost:8082/schemas/types  # EU

# Set both to IMPORT mode
curl -X PUT http://localhost:8081/mode -H "Content-Type: application/json" --data '{"mode":"IMPORT"}'
curl -X PUT http://localhost:8082/mode -H "Content-Type: application/json" --data '{"mode":"IMPORT"}'
```

### Step 3: Update Deployment Config

**File:** `deployment-config-phase2.yaml`

```yaml
regions:
  us-west:
    schemaRegistryUrl: http://localhost:8081
    authType: none
    mode: IMPORT

  eu-central:
    schemaRegistryUrl: http://localhost:8082
    authType: none
    mode: IMPORT

deploymentRules:
  - context: default
    subjects: ["user-events-*", "order-events-*"]
    regions: ["us-west", "eu-central"]

  - context: default
    subjects: ["gdpr-*"]
    regions: ["eu-central"]
```

### Step 4: Create Region-Specific Schema

**File:** `SCHEMASTORE/default/gdpr-user-data-value/v1/schema.avsc`

```json
{
  "type": "record",
  "name": "GdprUserData",
  "namespace": "com.example.gdpr",
  "fields": [
    {
      "name": "userId",
      "type": "string"
    },
    {
      "name": "email",
      "type": "string"
    },
    {
      "name": "consentGiven",
      "type": "boolean"
    }
  ]
}
```

**File:** `SCHEMASTORE/default/gdpr-user-data-value/v1/metadata.json`

```json
{
  "schemaId": 10003,
  "subject": "gdpr-user-data-value",
  "version": 1,
  "context": "default",
  "schemaType": "AVRO",
  "compatibilityMode": "FULL",
  "deploymentRegions": ["eu-central"],
  "createdAt": "2025-11-13T11:00:00Z",
  "createdBy": "test-user",
  "description": "GDPR user data - EU only",
  "tags": ["gdpr", "pii"]
}
```

### Step 5: Test Multi-Region Deployment

```bash
# Run deployment script (to be created next)
./deployment-scripts/deploy.sh --config deployment-config-phase2.yaml

# Verify US has user-events and order-events
curl http://localhost:8081/subjects | jq .
# Expected: ["user-events-value", "order-events-value"]

# Verify EU has all schemas
curl http://localhost:8082/subjects | jq .
# Expected: ["user-events-value", "order-events-value", "gdpr-user-data-value"]

# Verify IDs match
curl http://localhost:8081/schemas/ids/10001 | jq .schema
curl http://localhost:8082/schemas/ids/10001 | jq .schema
# Should be identical
```

### Phase 2 Success Criteria

- ✅ Two Schema Registries running independently
- ✅ Both in IMPORT mode
- ✅ Deployment script deploys to correct regions
- ✅ GDPR schema only in EU region
- ✅ Schema IDs identical across regions
- ✅ Web UI shows both registries

---

## Phase 3: Confluent Cloud Integration

**Goal:** Add Confluent Cloud Schema Registry for cloud deployment testing

### Step 1: Create Confluent Cloud Account

1. Sign up at https://confluent.cloud
2. Create a new environment
3. Create a Schema Registry
4. Generate API credentials (API Key + Secret)

### Step 2: Update Deployment Config

**File:** `deployment-config-phase3.yaml`

```yaml
regions:
  us-west-local:
    schemaRegistryUrl: http://localhost:8081
    authType: none
    mode: IMPORT

  eu-central-local:
    schemaRegistryUrl: http://localhost:8082
    authType: none
    mode: IMPORT

  confluent-cloud:
    schemaRegistryUrl: https://psrc-xxxxx.us-east-2.aws.confluent.cloud
    authType: basic
    username: ${CCLOUD_SR_API_KEY}
    password: ${CCLOUD_SR_API_SECRET}
    mode: IMPORT

deploymentRules:
  - context: default
    subjects: ["user-events-*"]
    regions: ["us-west-local", "confluent-cloud"]

  - context: default
    subjects: ["order-events-*"]
    regions: ["us-west-local", "eu-central-local"]

  - context: default
    subjects: ["gdpr-*"]
    regions: ["eu-central-local", "confluent-cloud"]
```

### Step 3: Configure Environment Variables

```bash
# Create .env file
cat > .env << EOF
CCLOUD_SR_API_KEY=your_api_key_here
CCLOUD_SR_API_SECRET=your_api_secret_here
CCLOUD_SR_URL=https://psrc-xxxxx.us-east-2.aws.confluent.cloud
EOF

# Load environment variables
export $(cat .env | xargs)
```

### Step 4: Set Confluent Cloud to IMPORT Mode

```bash
# Set IMPORT mode on Confluent Cloud SR
curl -X PUT ${CCLOUD_SR_URL}/mode \
  -u "${CCLOUD_SR_API_KEY}:${CCLOUD_SR_API_SECRET}" \
  -H "Content-Type: application/json" \
  --data '{"mode":"IMPORT"}'

# Verify
curl -u "${CCLOUD_SR_API_KEY}:${CCLOUD_SR_API_SECRET}" \
  ${CCLOUD_SR_URL}/mode | jq .
```

### Step 5: Test Cloud Deployment

```bash
# Deploy to all regions including cloud
./deployment-scripts/deploy.sh --config deployment-config-phase3.yaml

# Verify cloud deployment
curl -u "${CCLOUD_SR_API_KEY}:${CCLOUD_SR_API_SECRET}" \
  ${CCLOUD_SR_URL}/subjects | jq .

# Expected: ["user-events-value", "gdpr-user-data-value"]
```

### Phase 3 Success Criteria

- ✅ Confluent Cloud Schema Registry configured
- ✅ Cloud SR in IMPORT mode
- ✅ Deployment to cloud succeeds
- ✅ Schema IDs match across local and cloud
- ✅ Region-specific deployment rules work

---

## Testing Procedures

### Test 1: ID Allocation Idempotency

```bash
# Allocate ID for schema twice
curl -X POST http://localhost:8080/api/v1/schema-ids/allocate \
  -H "Content-Type: application/json" \
  --data @test-schema-request.json

# Run again
curl -X POST http://localhost:8080/api/v1/schema-ids/allocate \
  -H "Content-Type: application/json" \
  --data @test-schema-request.json

# Both should return same ID
```

### Test 2: Compatibility Validation

```bash
# Add v2 schema that breaks compatibility
cat > SCHEMASTORE/default/user-events-value/v2/schema.avsc << EOF
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.example.events",
  "fields": [
    {
      "name": "userId",
      "type": "string"
    }
    // Removed eventType and timestamp - breaks BACKWARD compatibility
  ]
}
EOF

# Try to add via API - should fail
curl -X POST http://localhost:8080/api/v1/schemas \
  -H "Content-Type: application/json" \
  --data '{
    "subject": "user-events-value",
    "context": "default",
    "schemaType": "AVRO",
    "schemaContent": "...",
    "compatibilityMode": "BACKWARD",
    "deploymentRegions": ["us-west-local"]
  }'

# Expected: 409 Conflict with compatibility error details
```

### Test 3: Multi-Region Deployment

```bash
# Deploy to all regions
./deployment-scripts/deploy.sh --config deployment-config-phase2.yaml

# Verify each region
for port in 8081 8082; do
  echo "=== Schema Registry on port $port ==="
  curl http://localhost:$port/subjects | jq .
  echo
done

# Compare schemas by ID
curl http://localhost:8081/schemas/ids/10001 > /tmp/schema-us.json
curl http://localhost:8082/schemas/ids/10001 > /tmp/schema-eu.json
diff /tmp/schema-us.json /tmp/schema-eu.json
# Should show no differences
```

### Test 4: Web UI Health Check

```bash
# Start Web UI
cd web-ui && npm run dev

# Open browser to http://localhost:3000/health

# Expected UI elements:
# - List of all registered Schema Registries
# - Sync status for each schema
# - Alerts for missing/drifted schemas
# - Button to re-deploy
```

---

## Troubleshooting

### Issue: Schema Registry won't start

**Symptoms:** Container exits immediately

**Diagnosis:**
```bash
docker logs schema-registry-test
```

**Common Causes:**
1. Kafka not ready - wait longer before starting SR
2. Port already in use - check `netstat -an | grep 8081`
3. Invalid environment variables

**Solution:**
```bash
# Stop all
docker-compose down -v

# Start Kafka first
docker-compose up -d kafka

# Wait for Kafka
sleep 30

# Start Schema Registry
docker-compose up -d schema-registry
```

### Issue: Mode change fails

**Error:** "Cannot import since found existing subjects"

**Solution:**
```bash
# Use force flag
curl -X PUT "http://localhost:8081/mode?force=true" \
  -H "Content-Type: application/json" \
  --data '{"mode":"IMPORT"}'
```

### Issue: ID conflict error

**Error:** "Overwrite new schema with id X"

**Diagnosis:**
```bash
# Check what schema has that ID
curl http://localhost:8081/schemas/ids/10001 | jq .
```

**Solution:**
- Verify you're using correct schema content
- Check if ID was already allocated in database
- Use different ID or delete conflicting schema

### Issue: API service won't start

**Error:** "ModuleNotFoundError"

**Solution:**
```bash
cd api-service
pip install -r requirements.txt
```

### Issue: Web UI can't connect to API

**Error:** "Network Error"

**Diagnosis:**
```bash
# Check API is running
curl http://localhost:8080/health

# Check CORS settings in API
```

**Solution:**
```bash
# Update .env in web-ui/
REACT_APP_API_URL=http://localhost:8080/api/v1

# Restart Web UI
npm run dev
```

---

## Next Steps

After completing Phase 3:

1. **Implement CI/CD Pipeline** (GitHub Actions)
2. **Add Authentication** to API and Web UI
3. **Performance Testing** with 1000+ schemas
4. **Production Deployment** to Kubernetes
5. **Monitoring Setup** with Prometheus/Grafana

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Owner:** Platform Engineering Team
