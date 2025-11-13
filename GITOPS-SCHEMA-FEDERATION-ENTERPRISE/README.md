# GitOps Schema Federation Manager - Enterprise Edition

**Version:** 2.0 Enterprise
**Status:** Production Ready
**Last Updated:** 2025-11-13

---

## 🚀 Enterprise Overview

The **Enterprise Edition** extends the core GitOps Schema Federation Manager with **Unity Catalog integration** and **cross-platform schema management**, enabling universal schema governance across your entire data ecosystem.

### What's New in Enterprise Edition

| Feature | Community | Enterprise |
|---------|-----------|------------|
| Kafka Schema Registry | ✅ | ✅ |
| **Unity Catalog Import** | ❌ | ✅ |
| **Iceberg Schema Support** | ❌ | ✅ |
| **Schema Metadata Graph** | ❌ | ✅ |
| **Cross-Platform Comparison** | ❌ | ✅ |
| **Lineage Tracking** | ❌ | ✅ |
| Multi-Region Deployment | ✅ | ✅ Enhanced |
| Web UI | ✅ | ✅ Enhanced |
| **Neo4j Integration** | ❌ | ✅ |
| **Automated Discovery** | ❌ | ✅ |

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [enterprise-features.md](./enterprise-features.md) | Complete feature specification (87 pages) |
| [DEMO-SCENARIO.md](./DEMO-SCENARIO.md) | 30-minute interactive demo guide |
| [docker/](./docker/) | Docker Compose setup for full environment |
| [importers/](./importers/) | Unity Catalog and other platform importers |
| [tests/](./tests/) | Comprehensive integration tests |

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   Data Sources                               │
├──────────────────────────────────────────────────────────────┤
│  Kafka SR  │  Unity Catalog  │  Snowflake  │  BigQuery      │
└──────┬─────────────┬───────────────┬──────────────┬──────────┘
       │             │               │              │
       └─────────────┼───────────────┴──────────────┘
                     ▼
    ┌────────────────────────────────────────┐
    │  GitOps Schema Federation Manager      │
    │           Enterprise Edition            │
    ├────────────────────────────────────────┤
    │  • Unity Catalog Importer              │
    │  • Schema Transformer                  │
    │  • Comparison Engine                   │
    │  • Lineage Tracker                     │
    │  • Metadata Graph (Neo4j)              │
    │  • Global Schema ID Allocator          │
    └────────────────┬───────────────────────┘
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
  ┌────────┐   ┌─────────┐  ┌──────────┐
  │ Web UI │   │   API   │  │ Git Repo │
  └────────┘   └─────────┘  └──────────┘
                                │
                                ▼
                          SCHEMASTORE/
                          ├── kafka/
                          ├── unity-catalog/
                          ├── snowflake/
                          └── schema-graph/
```

---

## 🚀 Quick Start

### Prerequisites

```bash
docker --version          # >= 20.10
docker-compose --version  # >= 2.0
python3 --version         # >= 3.10
```

### Start Enterprise Environment

```bash
# Navigate to enterprise directory
cd GITOPS-SCHEMA-FEDERATION-ENTERPRISE

# Start all services (Kafka, Unity Catalog, Neo4j, etc.)
docker-compose -f docker/docker-compose-enterprise.yml up -d

# Wait for services to be ready (2-3 minutes)
docker-compose -f docker/docker-compose-enterprise.yml logs -f

# Verify services
./scripts/health-check.sh
```

### Import Your First Unity Catalog Table

```bash
# Set Schema Registry to IMPORT mode
curl -X PUT http://localhost:8081/mode \
  -H "Content-Type: application/json" \
  --data '{"mode":"IMPORT"}'

# Import a Unity Catalog table
docker exec -it gitops-api-enterprise \
  python3 -m importers.unity_catalog_importer \
    --uc-url http://unity-catalog:8080 \
    --schema-id-service http://localhost:8090 \
    --schemastore-path /app/SCHEMASTORE \
    import-table \
    --catalog main \
    --schema bronze \
    --table users

# Verify import
ls -la SCHEMASTORE/unity-catalog/main/bronze/users/v1/
```

### Access Web UI

```bash
# Open in browser
open http://localhost:3000

# Navigate to:
# - Dashboard: Overview of all schemas
# - Schema Browser: Explore imported schemas
# - Lineage: View Neo4j graph
```

---

## 🎯 Key Use Cases

### 1. Lakehouse to Streaming Integration

**Scenario:** You have Iceberg tables in Unity Catalog and want to sync schemas with Kafka

**Solution:**
```bash
# Import Unity Catalog table
python3 -m importers.unity_catalog_importer import-table \
  --catalog main --schema bronze --table users

# Generate Kafka-compatible Avro schema (automatic)
# Schema ID 20001 allocated globally

# Deploy to Kafka Schema Registry
./deployment-scripts/deploy.sh --subject main.bronze.users
```

**Result:**
- ✅ Iceberg table schema → Avro schema conversion
- ✅ Global Schema ID (20001)
- ✅ Deployed to Kafka SR and SCHEMASTORE
- ✅ Lineage tracked in Neo4j

### 2. Cross-Platform Schema Comparison

**Scenario:** Verify Kafka and Unity Catalog schemas match

**Solution:**
```bash
# Compare schemas
curl -X POST http://localhost:8090/api/v1/schemas/compare \
  -H "Content-Type: application/json" \
  --data '{
    "source1": {"platform": "kafka", "subject": "user-events-value"},
    "source2": {"platform": "unity-catalog", "table": "main.bronze.users"}
  }'
```

**Result:**
- Field-level diff
- Compatibility analysis
- Migration recommendations

### 3. Schema Lineage Tracking

**Scenario:** Understand data flow from Kafka to Lakehouse to Analytics

**Solution:**
```bash
# Query lineage via API
curl http://localhost:8090/api/v1/graph/lineage?subject=main.bronze.users

# Visualize in Neo4j Browser
open http://localhost:7474
# Run: MATCH path = (:KafkaTopic)-[*]->(:Table) RETURN path
```

**Result:**
- Complete data lineage graph
- Impact analysis
- Dependency tracking

### 4. Automated Schema Discovery

**Scenario:** Discover all tables in Unity Catalog

**Solution:**
```bash
# Run discovery
python3 -m importers.unity_catalog_importer discover \
  --uc-url http://unity-catalog:8080 \
  --schema-id-service http://localhost:8090 \
  --schemastore-path ./SCHEMASTORE
```

**Result:**
- All catalogs, schemas, tables discovered
- JSON output for automation
- Can be scheduled via cron

---

## 📁 SCHEMASTORE Structure (Extended)

```
SCHEMASTORE/
├── kafka/                              # Kafka schemas (from core)
│   └── default/
│       └── user-events-value/
│           └── v1/
│               ├── schema.avsc
│               └── metadata.json
│
├── unity-catalog/                      # Unity Catalog schemas (NEW)
│   └── main/                           # Catalog
│       └── bronze/                     # Schema (namespace)
│           └── users/                  # Table
│               └── v1/
│                   ├── schema.avsc              # Converted Avro
│                   ├── schema.iceberg.json      # Original Iceberg
│                   └── metadata.json            # Extended metadata
│
├── snowflake/                          # Snowflake schemas (future)
│   └── ANALYTICS_DB/
│       └── PUBLIC/
│           └── DIM_USERS/
│               └── v1/
│                   ├── schema.avsc
│                   ├── schema.sql
│                   └── metadata.json
│
└── schema-graph/                       # Metadata graph (NEW)
    ├── nodes.json                      # All schema nodes
    ├── edges.json                      # Relationships
    └── lineage.json                    # Lineage data
```

---

## 🧪 Testing

### Run Integration Tests

```bash
# Navigate to tests directory
cd tests

# Install test dependencies
pip install pytest requests

# Run all tests
pytest -v

# Run specific test suite
pytest test_unity_catalog_import.py -v

# Run with coverage
pytest --cov=importers --cov-report=html
```

### Test Suites

| Test Suite | Coverage |
|------------|----------|
| `test_unity_catalog_import.py` | Unity Catalog connection, import, SCHEMASTORE |
| `test_schema_comparison.py` | Cross-platform schema comparison |
| `test_lineage.py` | Neo4j lineage tracking |
| `test_drift_detection.py` | Schema drift detection |

---

## 🎬 Demo Scenario

Follow the [DEMO-SCENARIO.md](./DEMO-SCENARIO.md) for a complete 30-minute demo showing:

1. **Unity Catalog Import** (10 min)
   - Import Iceberg tables
   - Automatic Avro conversion
   - Global Schema ID allocation

2. **Cross-Platform Comparison** (8 min)
   - Compare Kafka vs. Unity Catalog schemas
   - Field-level diff analysis
   - Compatibility reports

3. **Lineage Visualization** (7 min)
   - Neo4j graph exploration
   - Cypher queries
   - Impact analysis

4. **Drift Detection** (5 min)
   - Detect schema changes
   - Generate drift reports
   - Automated alerts

---

## 🔧 Configuration

### Unity Catalog Connection

**File:** `deployment-config-enterprise.yaml`

```yaml
platforms:
  unity-catalog:
    url: http://unity-catalog:8080
    auth:
      type: token
      token: ${UC_AUTH_TOKEN}
    catalogs:
      - main
      - analytics
    import:
      enabled: true
      schedule: "0 */6 * * *"  # Every 6 hours
```

### Neo4j Graph Database

**Connection:**
- URL: http://localhost:7474
- Bolt: bolt://localhost:7687
- User: neo4j
- Password: password123

**Cypher Queries:**
```cypher
// Find all downstream tables from a Kafka topic
MATCH path = (k:KafkaTopic {name: "user-events-cdc"})
             -[:CONSUMED_BY|WRITES_TO*1..5]->
             (t:Table)
RETURN path

// Find all PII schemas
MATCH (s:Schema)
WHERE s.classification = "PII"
RETURN s.subject, s.platform, s.piiFields

// Impact analysis: what breaks if I change this schema?
MATCH path = (s:Schema {subject: "user-events-value", version: 2})
             -[:DEPLOYED_TO|DEFINES|CONSUMED_BY*1..3]->
             (downstream)
RETURN path
```

---

## 📊 Monitoring & Observability

### Health Checks

```bash
# Check all services
curl http://localhost:8090/api/v1/health

# Check Unity Catalog
curl http://localhost:8080/api/2.1/unity-catalog/catalogs

# Check Neo4j
curl http://localhost:7474

# Check Kafka Schema Registry
curl http://localhost:8081/subjects
```

### Metrics (Prometheus)

```yaml
# Exposed metrics
- gitops_schemas_total{platform="unity-catalog"}
- gitops_imports_total{status="success|failure"}
- gitops_drift_detected_total
- gitops_lineage_edges_total
```

### Logs

```bash
# View API service logs
docker logs gitops-api-enterprise -f

# View Unity Catalog logs
docker logs unity-catalog-enterprise -f

# View Neo4j logs
docker logs neo4j-enterprise -f
```

---

## 🔐 Security & Governance

### Authentication

**Unity Catalog:**
- Personal Access Token (PAT)
- OAuth 2.0 (production)
- Service Principal

**Kafka Schema Registry:**
- Basic Auth
- SSL/TLS
- SASL

**Neo4j:**
- Username/password
- LDAP (enterprise)

### PII Detection

```json
// Metadata includes PII fields
{
  "governance": {
    "classification": "PII",
    "piiFields": ["email", "phone_number", "ssn"],
    "retentionDays": 365,
    "encryptionRequired": true
  }
}
```

### Audit Trail

All operations logged:
- Schema imports (who, when, from where)
- Comparisons run
- Deployments executed
- Drift detected

---

## 🛠️ Troubleshooting

### Unity Catalog Connection Fails

**Error:** `Connection refused to Unity Catalog`

**Solution:**
```bash
# Check Unity Catalog is running
docker ps | grep unity-catalog

# Check logs
docker logs unity-catalog-enterprise

# Verify PostgreSQL metastore
docker logs postgres-uc-enterprise

# Restart services
docker-compose -f docker/docker-compose-enterprise.yml restart unity-catalog
```

### Import Fails with "Schema ID allocation error"

**Error:** `Failed to allocate schema ID`

**Solution:**
```bash
# Check Schema ID service
curl http://localhost:8090/health

# Check database connection
docker exec -it postgres-gitops-enterprise \
  psql -U gitops -d schema_federation -c "SELECT * FROM schema_ids LIMIT 5;"

# Restart API service
docker-compose -f docker/docker-compose-enterprise.yml restart gitops-api
```

### Neo4j Graph Not Loading

**Error:** `Cannot connect to Neo4j`

**Solution:**
```bash
# Check Neo4j is running
docker ps | grep neo4j

# Access Neo4j browser
open http://localhost:7474

# Reset password if needed
docker exec -it neo4j-enterprise cypher-shell -u neo4j -p password123
```

---

## 📈 Performance & Scaling

### Recommended Resources

**Development:**
- 4 CPU cores
- 8GB RAM
- 20GB disk

**Production:**
- 8+ CPU cores
- 16GB+ RAM
- 100GB+ SSD disk

### Scaling Guidelines

| Component | Scaling Strategy |
|-----------|------------------|
| API Service | Horizontal (multiple replicas) |
| PostgreSQL | Vertical + read replicas |
| Neo4j | Vertical + sharding |
| MinIO | Distributed mode (4+ nodes) |

### Performance Tuning

```yaml
# API Service
API_WORKERS: 4
API_THREADS: 2

# PostgreSQL
POSTGRES_MAX_CONNECTIONS: 100
POSTGRES_SHARED_BUFFERS: 2GB

# Neo4j
NEO4J_dbms_memory_heap_max__size: 4G
NEO4J_dbms_memory_pagecache_size: 2G
```

---

## 🗺️ Roadmap

### Q1 2025 (Current)
- ✅ Unity Catalog integration
- ✅ Iceberg schema support
- ✅ Neo4j metadata graph
- ✅ Cross-platform comparison
- 🔄 Web UI enhancements

### Q2 2025
- ⏳ Snowflake connector
- ⏳ BigQuery connector
- ⏳ AWS Glue integration
- ⏳ Advanced governance policies
- ⏳ ML-powered schema recommendations

### Q3 2025
- ⏳ Real-time schema sync
- ⏳ Schema marketplace
- ⏳ Multi-tenancy support
- ⏳ Advanced RBAC
- ⏳ Custom transformations

---

## 💼 Enterprise Support

### Getting Help

- **Documentation**: See `/docs` directory
- **GitHub Issues**: Create issue with `[ENTERPRISE]` tag
- **Email**: enterprise-support@example.com
- **Slack**: #schema-federation-enterprise

### Professional Services

- Architecture consultation
- Custom connector development
- Performance tuning
- Training and enablement

---

## 📝 License

**Enterprise Edition** requires a commercial license.

**Community Edition** (without Unity Catalog) is Apache 2.0 licensed.

Contact: licensing@example.com

---

## 🙏 Acknowledgments

Built with:
- Apache Kafka & Confluent Platform
- Unity Catalog (Databricks)
- Apache Iceberg
- Neo4j Graph Database
- Python FastAPI
- React

---

**Version:** 2.0.0-enterprise
**Last Updated:** 2025-11-13
**Maintained By:** Platform Engineering Team - Enterprise Division
**Support:** enterprise-support@example.com
