# GitOps Schema Federation Manager - Enterprise Demo Scenario

**Version:** 2.0 Enterprise
**Date:** 2025-11-13
**Duration:** 30 minutes

---

## Table of Contents

1. [Demo Overview](#demo-overview)
2. [Prerequisites](#prerequisites)
3. [Environment Setup](#environment-setup)
4. [Demo Scenario](#demo-scenario)
5. [Key Demonstrations](#key-demonstrations)
6. [Testing Procedures](#testing-procedures)
7. [Cleanup](#cleanup)

---

## Demo Overview

This demo showcases the **GitOps Schema Federation Manager Enterprise Edition** capabilities:

### Demo Highlights

1. **Multi-Platform Schema Management**
   - Kafka Schema Registry
   - Unity Catalog (Iceberg tables)
   - Centralized SCHEMASTORE in Git

2. **Unity Catalog Integration**
   - Import Iceberg table schemas
   - Automatic Avro conversion
   - Global Schema ID allocation

3. **Schema Lineage Tracking**
   - Kafka Topic → Unity Catalog Table
   - Upstream/downstream dependencies
   - Transformation tracking

4. **Cross-Platform Comparison**
   - Kafka schema vs. Unity Catalog table schema
   - Compatibility analysis
   - Drift detection

5. **Metadata Graph Visualization**
   - Neo4j graph database
   - Lineage queries
   - Impact analysis

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       Demo Environment                        │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  Kafka + Schema Registry  ──┐                                │
│                             │                                 │
│  Unity Catalog + MinIO   ───┼──→  GitOps Schema Manager  ──→ │
│                             │      - API Service             │
│  Neo4j Graph Database    ──┘      - Web UI                   │
│                                    - SCHEMASTORE (Git)        │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

### Required Software

```bash
docker --version          # >= 20.10
docker-compose --version  # >= 2.0
python3 --version         # >= 3.10
curl --version           # Any recent version
jq --version             # 1.6+
```

### Recommended Tools

- **Browser**: Chrome, Firefox, or Safari (for Web UI and Neo4j Browser)
- **Terminal**: iTerm2, Terminal, or Windows Terminal
- **Code Editor**: VS Code (optional, for viewing schemas)

### System Resources

- **CPU**: 4+ cores recommended
- **RAM**: 8GB minimum, 16GB recommended
- **Disk**: 10GB free space

---

## Environment Setup

### Step 1: Clone Repository

```bash
git clone https://github.com/your-org/global-schema-registry.git
cd global-schema-registry/GITOPS-SCHEMA-FEDERATION-ENTERPRISE
```

### Step 2: Start Infrastructure

```bash
# Start all services
docker-compose -f docker/docker-compose-enterprise.yml up -d

# Monitor startup logs
docker-compose -f docker/docker-compose-enterprise.yml logs -f

# Wait for all services to be healthy (2-3 minutes)
```

### Step 3: Verify Services

```bash
# Kafka
curl http://localhost:9092 || echo "Kafka not ready yet"

# Schema Registry
curl http://localhost:8081/schemas/types
# Expected: ["JSON","PROTOBUF","AVRO"]

# Unity Catalog
curl http://localhost:8080/api/2.1/unity-catalog/catalogs
# Expected: {"catalogs":[...]}

# Neo4j
curl http://localhost:7474
# Should return Neo4j Browser HTML

# GitOps API
curl http://localhost:8090/health
# Expected: {"status":"healthy"}

# GitOps Web UI
open http://localhost:3000
```

### Step 4: Set Schema Registry to IMPORT Mode

```bash
# Set IMPORT mode
curl -X PUT http://localhost:8081/mode \
  -H "Content-Type: application/json" \
  --data '{"mode":"IMPORT"}'

# Verify
curl http://localhost:8081/mode | jq .
# Expected: {"mode":"IMPORT"}
```

### Step 5: Initialize Demo Data

The demo setup containers automatically create:

- **Unity Catalog Tables**:
  - `main.bronze.users`
  - `main.bronze.orders`
  - `main.silver.users_enriched`
  - `analytics.reports.user_activity_daily`

- **Kafka Topics**:
  - `user-events`
  - `user-events-cdc`
  - `order-events`

Verify creation:

```bash
# List Unity Catalog tables
docker exec -it unity-catalog-enterprise \
  curl http://localhost:8080/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=bronze \
  | jq '.tables[].name'

# List Kafka topics
docker exec -it kafka-enterprise \
  kafka-topics --bootstrap-server localhost:29092 --list
```

---

## Demo Scenario

### Scenario: E-Commerce Platform Schema Federation

**Context:**
You're managing an e-commerce platform with:
- **Kafka**: Real-time event streaming (user events, orders)
- **Data Lakehouse**: Unity Catalog managing Iceberg tables
- **Analytics**: Downstream reporting and BI

**Challenge:**
- Schemas are scattered across platforms
- No visibility into schema lineage
- Manual schema synchronization is error-prone
- Compliance requires centralized schema governance

**Solution:**
GitOps Schema Federation Manager Enterprise Edition!

---

## Key Demonstrations

### Demo 1: Import Unity Catalog Schemas (10 min)

**Objective:** Import Iceberg table schemas into centralized SCHEMASTORE

#### Step 1: Discover Unity Catalog Tables

```bash
# List all catalogs
curl http://localhost:8080/api/2.1/unity-catalog/catalogs | jq '.catalogs[].name'

# List tables in main.bronze
curl "http://localhost:8080/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=bronze" \
  | jq '.tables[].name'
```

**Expected Output:**
```
"users"
"orders"
```

#### Step 2: Import Users Table

```bash
# Using the Unity Catalog importer
docker exec -it gitops-api-enterprise python3 -m importers.unity_catalog_importer \
  --uc-url http://unity-catalog:8080 \
  --schema-id-service http://localhost:8090 \
  --schemastore-path /app/SCHEMASTORE \
  import-table \
  --catalog main \
  --schema bronze \
  --table users
```

**Expected Output:**
```
✓ Imported main.bronze.users → Schema ID 20001
✓ Saved schema to /app/SCHEMASTORE/unity-catalog/main/bronze/users/v1
```

#### Step 3: Verify Import

```bash
# Check SCHEMASTORE
ls -la SCHEMASTORE/unity-catalog/main/bronze/users/v1/

# Files created:
# - schema.avsc              (Avro schema)
# - schema.iceberg.json      (Original Unity Catalog schema)
# - metadata.json            (Extended metadata)

# View Avro schema
cat SCHEMASTORE/unity-catalog/main/bronze/users/v1/schema.avsc | jq .
```

**Expected Schema:**
```json
{
  "type": "record",
  "name": "users",
  "namespace": "main.bronze",
  "doc": "Imported from Unity Catalog: main.bronze.users",
  "fields": [
    {
      "name": "user_id",
      "type": "string",
      "doc": "Imported from Unity Catalog"
    },
    {
      "name": "email",
      "type": ["null", "string"],
      "default": null,
      "doc": "Imported from Unity Catalog"
    },
    {
      "name": "created_at",
      "type": "long",
      "logicalType": "timestamp-micros",
      "doc": "Imported from Unity Catalog"
    }
    // ... more fields
  ]
}
```

#### Step 4: Import All Tables in Schema

```bash
# Import all tables from main.bronze
docker exec -it gitops-api-enterprise python3 -m importers.unity_catalog_importer \
  --uc-url http://unity-catalog:8080 \
  --schema-id-service http://localhost:8090 \
  --schemastore-path /app/SCHEMASTORE \
  import-schema \
  --catalog main \
  --schema bronze
```

**Expected Output:**
```
✓ Imported main.bronze.users → Schema ID 20001
✓ Imported main.bronze.orders → Schema ID 20002
Imported 2 tables
```

---

### Demo 2: Compare Kafka and Unity Catalog Schemas (8 min)

**Objective:** Show schema comparison across platforms

#### Step 1: Register Kafka Schema Manually

```bash
# Create user-events schema for Kafka
cat > /tmp/user-events-kafka.avsc << 'EOF'
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.ecommerce.events",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-micros"},
    {"name": "properties", "type": {"type": "map", "values": "string"}}
  ]
}
EOF

# Register in Kafka Schema Registry with custom ID
SCHEMA_JSON=$(cat /tmp/user-events-kafka.avsc | jq -c .)

curl -X POST http://localhost:8081/subjects/user-events-value/versions \
  -H "Content-Type: application/json" \
  --data "{
    \"schema\": $SCHEMA_JSON,
    \"schemaType\": \"AVRO\",
    \"id\": 10001
  }" | jq .
```

**Expected Response:**
```json
{"id": 10001}
```

#### Step 2: Compare Schemas via API

```bash
# Compare Kafka schema vs Unity Catalog table
curl -X POST http://localhost:8090/api/v1/schemas/compare \
  -H "Content-Type: application/json" \
  --data '{
    "source1": {
      "platform": "kafka",
      "subject": "user-events-value",
      "version": 1
    },
    "source2": {
      "platform": "unity-catalog",
      "table": "main.bronze.users",
      "version": 1
    }
  }' | jq .
```

**Expected Comparison Report:**
```json
{
  "compatibility": "PARTIAL",
  "field_diffs": [
    {
      "field": "user_id",
      "change": "NONE",
      "compatible": true
    },
    {
      "field": "email",
      "change": "ADDED",
      "source1_type": null,
      "source2_type": "string",
      "compatible": true,
      "impact": "NONE"
    },
    {
      "field": "event_type",
      "change": "REMOVED",
      "source1_type": "string",
      "source2_type": null,
      "compatible": false,
      "impact": "MEDIUM"
    }
  ],
  "summary": {
    "total_fields": 8,
    "unchanged": 1,
    "added": 6,
    "removed": 3,
    "type_changed": 0,
    "breaking_changes": 0
  },
  "recommendations": [
    "Consider aligning field names for consistency",
    "Added fields in Unity Catalog do not break compatibility"
  ]
}
```

---

### Demo 3: Schema Lineage Visualization (7 min)

**Objective:** Show end-to-end data lineage

#### Step 1: Add Lineage Information

Lineage is automatically captured during import. Let's query it:

```bash
# Get lineage for users table
curl http://localhost:8090/api/v1/graph/lineage?subject=main.bronze.users | jq .
```

**Expected Lineage:**
```json
{
  "subject": "main.bronze.users",
  "upstream": [
    {
      "type": "kafka-topic",
      "name": "user-events-cdc",
      "schemaId": 10001
    }
  ],
  "downstream": [
    {
      "type": "unity-table",
      "name": "main.silver.users_enriched",
      "schemaId": 20003
    }
  ],
  "transformations": [
    {
      "type": "spark-job",
      "name": "bronze_to_silver_users"
    }
  ]
}
```

#### Step 2: Visualize in Neo4j Browser

```bash
# Open Neo4j Browser
open http://localhost:7474

# Login: neo4j / password123

# Run Cypher query
MATCH path = (k:KafkaTopic {name: "user-events-cdc"})
             -[*1..3]->
             (t:Table {platform: "unity-catalog"})
RETURN path
```

**Expected Visualization:**
A graph showing:
- Kafka Topic (user-events-cdc)
- → Unity Table (main.bronze.users)
- → Unity Table (main.silver.users_enriched)

---

### Demo 4: Drift Detection (5 min)

**Objective:** Detect schema drift across platforms

#### Step 1: Modify Unity Catalog Table

```bash
# Simulate schema evolution - add new column to users table
# (In production, this would be done via ALTER TABLE)

# For demo, we'll create a modified schema in SCHEMASTORE
# and show drift detection
```

#### Step 2: Run Drift Detection

```bash
curl http://localhost:8090/api/v1/schemas/drift-report | jq .
```

**Expected Drift Report:**
```json
{
  "total_schemas": 4,
  "in_sync": 3,
  "drifted": 1,
  "drifts": [
    {
      "subject": "main.bronze.users",
      "platform": "unity-catalog",
      "issue": "Schema in Unity Catalog has additional field 'phone_number' not in SCHEMASTORE",
      "severity": "MEDIUM",
      "recommendation": "Re-import schema to capture changes"
    }
  ]
}
```

---

### Demo 5: Web UI Exploration (5 min)

**Objective:** Show user interface capabilities

#### Navigate to Web UI

```bash
open http://localhost:3000
```

#### Dashboard Features

1. **Overview**
   - Total schemas: 6 (2 Kafka + 4 Unity Catalog)
   - Platforms: Kafka, Unity Catalog
   - Recent imports

2. **Schema Browser**
   - Tree view: Platform → Context → Subject
   - Expand `unity-catalog` → `main` → `bronze` → `users`
   - View Avro schema with syntax highlighting
   - View original Iceberg schema
   - View metadata (lineage, governance, etc.)

3. **Platform Connections**
   - Kafka Schema Registry: http://localhost:8081 (Connected ✓)
   - Unity Catalog: http://localhost:8080 (Connected ✓)
   - Test connection buttons

4. **Health Dashboard**
   - Sync status table:
     | Subject | Kafka SR | Unity Catalog | Git | Status |
     |---------|----------|---------------|-----|--------|
     | user-events-value | ✓ | - | ✓ | IN_SYNC |
     | main.bronze.users | - | ✓ | ✓ | IN_SYNC |

5. **Import Wizard**
   - Select platform: Unity Catalog
   - Select catalog: main
   - Select schema: bronze
   - Select tables: [x] users, [x] orders
   - Click "Import" → Success!

---

## Testing Procedures

### Test 1: End-to-End Import Test

```bash
# Test script location
cd GITOPS-SCHEMA-FEDERATION-ENTERPRISE/tests

# Run integration test
pytest test_unity_catalog_import.py -v
```

**Test Coverage:**
- ✓ Connect to Unity Catalog
- ✓ List catalogs and tables
- ✓ Import single table
- ✓ Verify schema ID allocation
- ✓ Verify SCHEMASTORE files created
- ✓ Verify metadata correctness
- ✓ Import multiple tables
- ✓ Test idempotency (re-import same table)

### Test 2: Schema Comparison Test

```bash
pytest test_schema_comparison.py -v
```

**Test Coverage:**
- ✓ Compare identical schemas → COMPATIBLE
- ✓ Compare with added field → BACKWARD_COMPATIBLE
- ✓ Compare with removed field → BREAKING
- ✓ Compare with type change → BREAKING
- ✓ Generate recommendations

### Test 3: Lineage Tracking Test

```bash
pytest test_lineage.py -v
```

**Test Coverage:**
- ✓ Create lineage graph in Neo4j
- ✓ Query upstream dependencies
- ✓ Query downstream dependencies
- ✓ Find all PII schemas
- ✓ Impact analysis query

### Test 4: Drift Detection Test

```bash
pytest test_drift_detection.py -v
```

**Test Coverage:**
- ✓ Detect added field in UC
- ✓ Detect removed field
- ✓ Detect type change
- ✓ Generate drift report
- ✓ Suggest remediation

---

## Cleanup

### Stop All Services

```bash
docker-compose -f docker/docker-compose-enterprise.yml down
```

### Remove Volumes (Complete Reset)

```bash
docker-compose -f docker/docker-compose-enterprise.yml down -v
```

### Selective Cleanup

```bash
# Keep data, stop services
docker-compose -f docker/docker-compose-enterprise.yml stop

# Restart
docker-compose -f docker/docker-compose-enterprise.yml start
```

---

## Demo Script (Presenter Guide)

### Introduction (2 min)

"Today I'm going to show you the GitOps Schema Federation Manager Enterprise Edition, which solves a critical problem: managing schemas across multiple data platforms.

In modern data architectures, we have:
- Kafka for streaming
- Unity Catalog for data lakehouse
- Various data warehouses

Each maintains its own schemas, leading to:
- Schema drift
- Manual synchronization
- No lineage visibility
- Compliance challenges

Let's see how we solve this."

### Demo Flow (25 min)

**Part 1: Setup (3 min)**
- Show docker-compose.yml
- Highlight services: Kafka, Unity Catalog, Neo4j
- Start environment
- Verify all services healthy

**Part 2: Import Unity Catalog Schemas (7 min)**
- Show Unity Catalog tables: `main.bronze.users`
- Run importer CLI
- Show SCHEMASTORE structure
- Show Avro conversion
- Highlight global Schema ID (20001)

**Part 3: Cross-Platform Comparison (5 min)**
- Register Kafka schema (ID 10001)
- Run comparison API
- Show field-level diff
- Explain compatibility analysis

**Part 4: Lineage Visualization (5 min)**
- Open Neo4j Browser
- Run Cypher query
- Show Kafka → Unity Catalog → Silver layer
- Explain impact analysis

**Part 5: Web UI Walkthrough (5 min)**
- Open dashboard
- Browse schemas
- Show health monitoring
- Demonstrate drift detection

### Conclusion (3 min)

"As you've seen, GitOps Schema Federation Manager Enterprise provides:
1. ✓ Unified schema management across platforms
2. ✓ Automated import and conversion
3. ✓ Complete lineage visibility
4. ✓ Drift detection and governance
5. ✓ All managed through Git for auditability

This reduces operational overhead, improves data quality, and ensures compliance.

Questions?"

---

## Appendix: Common Issues

### Issue: Unity Catalog not starting

**Symptoms:** Container exits or HTTP 500 errors

**Solution:**
```bash
# Check logs
docker logs unity-catalog-enterprise

# Verify PostgreSQL is ready
docker logs postgres-uc-enterprise

# Restart
docker-compose -f docker/docker-compose-enterprise.yml restart unity-catalog
```

### Issue: Schema import fails

**Error:** "Connection refused" to Schema ID service

**Solution:**
```bash
# Check API service is running
docker ps | grep gitops-api

# Check logs
docker logs gitops-api-enterprise

# Verify database connection
docker exec -it postgres-gitops-enterprise psql -U gitops -d schema_federation -c "\dt"
```

### Issue: Neo4j authentication fails

**Error:** "Invalid username or password"

**Solution:**
- Default credentials: `neo4j` / `password123`
- Clear browser cache
- Check environment variables in docker-compose.yml

---

**Demo Version:** 2.0 Enterprise
**Last Updated:** 2025-11-13
**Demo Owner:** Platform Engineering Team
**Support:** platform-team@example.com
