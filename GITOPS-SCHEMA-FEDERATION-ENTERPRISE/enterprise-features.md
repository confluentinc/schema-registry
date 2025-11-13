# GitOps Schema Federation Manager - Enterprise Edition

**Version:** 2.0 Enterprise
**Date:** 2025-11-13
**Status:** Planning & Implementation

---

## Table of Contents

1. [Enterprise Vision](#enterprise-vision)
2. [New Features](#new-features)
3. [Unity Catalog Integration](#unity-catalog-integration)
4. [Schema Metadata Graph](#schema-metadata-graph)
5. [Multi-Source Schema Management](#multi-source-schema-management)
6. [Architecture Extensions](#architecture-extensions)
7. [Implementation Plan](#implementation-plan)

---

## Enterprise Vision

**GitOps Schema Federation Manager Enterprise** extends the core platform to become a **universal schema management hub** across the entire data ecosystem, bridging:

- **Streaming Platforms**: Kafka Schema Registry
- **Data Lakehouses**: Unity Catalog (Databricks, Iceberg)
- **Cloud Data Warehouses**: Snowflake, BigQuery, Redshift
- **Data Catalogs**: AWS Glue, Azure Purview
- **API Schemas**: OpenAPI, GraphQL

### Key Goals

1. **Unified Schema View**: Single pane of glass for all schemas across platforms
2. **Cross-Platform Lineage**: Track schema evolution and data flow across systems
3. **Automated Discovery**: Import schemas from diverse sources
4. **Schema Governance**: Enforce policies across the data landscape
5. **Metadata Graph**: Build comprehensive knowledge graph of data structures

---

## New Features

### 1. Unity Catalog Schema Importer

**Purpose:** Import table schemas from Unity Catalog-managed Iceberg tables

**Capabilities:**
- Discover catalogs, schemas, tables from Unity Catalog
- Import Iceberg table schemas into SCHEMASTORE
- Map Unity Catalog types to Avro/Protobuf equivalents
- Preserve lineage metadata (source table, catalog, namespace)
- Bidirectional sync: UC → GitOps and GitOps → UC

**Use Cases:**
- Import Databricks Delta Lake schemas
- Sync Iceberg table definitions
- Maintain schema consistency between streaming and lakehouse
- Generate Kafka schemas from table definitions

### 2. Schema Metadata Graph

**Purpose:** Build comprehensive graph of schema relationships and lineage

**Graph Elements:**

**Nodes:**
- Schemas (with all versions)
- Tables (Unity Catalog, Snowflake, etc.)
- Topics (Kafka)
- APIs (REST, GraphQL)
- Producers/Consumers
- Transformations (Spark, Flink, DBT)

**Edges:**
- Derived From (lineage)
- Compatible With
- References (schema dependencies)
- Deployed To (region mapping)
- Consumes/Produces
- Transforms

**Query Capabilities:**
- "What tables produce data for this Kafka topic?"
- "Which downstream systems are affected if I change this schema?"
- "Show me all schemas with PII fields"
- "Find schemas not used in the last 90 days"

### 3. Multi-Source Schema Comparison

**Purpose:** Compare schemas across different platforms

**Comparisons:**
- Kafka Schema (Avro) ↔ Unity Catalog Table (Iceberg)
- Kafka Schema ↔ Snowflake Table
- Unity Catalog ↔ BigQuery
- Structural diff with field-level mapping

**Outputs:**
- Compatibility report
- Migration recommendations
- Schema translation code generation
- Drift detection alerts

### 4. Schema Discovery Service

**Purpose:** Automatically discover schemas from connected systems

**Discovery Sources:**
- Unity Catalog (catalogs, schemas, tables)
- Kafka Schema Registry (subjects, versions)
- Snowflake (databases, schemas, tables)
- AWS Glue Data Catalog
- Databricks Metastore
- PostgreSQL information_schema
- MongoDB collections

**Discovery Modes:**
- **Full Scan**: Discover all schemas
- **Incremental**: Detect new/changed schemas
- **Scheduled**: Run discovery on cron schedule
- **Event-Driven**: React to schema change events

### 5. Schema Transformation Pipeline

**Purpose:** Convert schemas between formats

**Transformations:**
- Iceberg Schema → Avro
- Avro → Iceberg Schema
- Protobuf → Avro
- JSON Schema → Avro
- SQL DDL → Avro
- Parquet → Avro

**Features:**
- Type mapping rules (configurable)
- Field name normalization
- Metadata preservation
- Validation of transformed schemas

### 6. Governance & Policies

**Purpose:** Enforce schema governance rules

**Policy Types:**
- **PII Detection**: Flag fields containing sensitive data
- **Naming Conventions**: Enforce subject/field naming standards
- **Compatibility Rules**: Require specific compatibility modes
- **Approval Workflows**: Multi-stage schema approval
- **Tagging Requirements**: Mandate classification tags
- **Retention Policies**: Auto-archive old schema versions

**Policy Enforcement:**
- Pre-commit validation (Git hooks)
- CI/CD pipeline gates
- API-level rejection
- Alerting & notifications

### 7. Advanced Analytics

**Purpose:** Insights into schema landscape

**Metrics:**
- Schema count by source platform
- Schema evolution velocity
- Compatibility break frequency
- Dead schemas (unused)
- Coverage (% of tables with registered schemas)
- Top producers/consumers

**Dashboards:**
- Executive summary
- Platform health
- Schema lifecycle
- Lineage visualization

---

## Unity Catalog Integration

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  GitOps Schema Federation                    │
│                     Enterprise Edition                       │
└───────────────┬─────────────────────────────────────────────┘
                │
                ├─── Kafka Schema Registry (existing)
                │
                ├─── Unity Catalog Integration (NEW)
                │    │
                │    ├─── UC REST API Client
                │    ├─── Iceberg Schema Parser
                │    ├─── Type Mapping Engine
                │    └─── Lineage Tracker
                │
                ├─── Snowflake Connector (NEW)
                ├─── BigQuery Connector (future)
                └─── AWS Glue Connector (future)
```

### Unity Catalog REST API Client

**Endpoints Used:**

```python
# List catalogs
GET /api/2.1/unity-catalog/catalogs

# List schemas in catalog
GET /api/2.1/unity-catalog/schemas?catalog_name={catalog}

# List tables in schema
GET /api/2.1/unity-catalog/tables?catalog_name={catalog}&schema_name={schema}

# Get table details (including schema)
GET /api/2.1/unity-catalog/tables/{full_name}

# Get table lineage
GET /api/2.1/unity-catalog/lineage/table-lineage?table_name={full_name}
```

**Authentication:**
- Databricks PAT (Personal Access Token)
- OAuth 2.0 (for production)
- Service Principal

### Iceberg Schema Mapping

**Iceberg Types → Avro Types:**

| Iceberg Type | Avro Type | Notes |
|--------------|-----------|-------|
| boolean | boolean | Direct mapping |
| int | int | Direct mapping |
| long | long | Direct mapping |
| float | float | Direct mapping |
| double | double | Direct mapping |
| decimal(P,S) | bytes + logicalType: decimal | Precision & scale preserved |
| date | int + logicalType: date | Days since epoch |
| time | long + logicalType: time-micros | Microseconds since midnight |
| timestamp | long + logicalType: timestamp-micros | Microseconds since epoch |
| timestamptz | long + logicalType: timestamp-micros | With timezone info in metadata |
| string | string | Direct mapping |
| uuid | string + logicalType: uuid | Direct mapping |
| fixed(N) | fixed | Fixed-size binary |
| binary | bytes | Variable-length binary |
| struct | record | Nested record |
| list | array | Array of items |
| map | map | Key-value map |

**Example Mapping:**

**Iceberg Schema:**
```json
{
  "type": "struct",
  "fields": [
    {"id": 1, "name": "user_id", "type": "long", "required": true},
    {"id": 2, "name": "email", "type": "string", "required": false},
    {"id": 3, "name": "created_at", "type": "timestamp", "required": true},
    {"id": 4, "name": "metadata", "type": {
      "type": "struct",
      "fields": [
        {"id": 5, "name": "tags", "type": {"type": "list", "element": "string"}}
      ]
    }}
  ]
}
```

**Avro Schema (Generated):**
```json
{
  "type": "record",
  "name": "users",
  "namespace": "catalog.schema",
  "doc": "Imported from Unity Catalog: catalog.schema.users",
  "fields": [
    {
      "name": "user_id",
      "type": "long",
      "doc": "Source: Iceberg field ID 1"
    },
    {
      "name": "email",
      "type": ["null", "string"],
      "default": null,
      "doc": "Source: Iceberg field ID 2"
    },
    {
      "name": "created_at",
      "type": "long",
      "logicalType": "timestamp-micros",
      "doc": "Source: Iceberg field ID 3"
    },
    {
      "name": "metadata",
      "type": {
        "type": "record",
        "name": "metadata",
        "fields": [
          {
            "name": "tags",
            "type": {
              "type": "array",
              "items": "string"
            },
            "doc": "Source: Iceberg field ID 5"
          }
        ]
      },
      "doc": "Source: Iceberg field ID 4"
    }
  ]
}
```

### SCHEMASTORE Extension for Multi-Source

**Extended Directory Structure:**

```
SCHEMASTORE/
├── kafka/                          # Kafka schemas (existing)
│   └── default/
│       └── user-events-value/
│           └── v1/
│               ├── schema.avsc
│               └── metadata.json
│
├── unity-catalog/                  # Unity Catalog schemas (NEW)
│   └── main/                       # Catalog name
│       └── bronze/                 # Schema (namespace)
│           └── users/              # Table name
│               └── v1/
│                   ├── schema.avsc         # Converted to Avro
│                   ├── schema.iceberg.json # Original Iceberg schema
│                   └── metadata.json
│
├── snowflake/                      # Snowflake schemas (NEW)
│   └── ANALYTICS_DB/
│       └── PUBLIC/
│           └── DIM_USERS/
│               └── v1/
│                   ├── schema.avsc
│                   ├── schema.sql
│                   └── metadata.json
│
└── schema-graph/                   # Schema metadata graph (NEW)
    ├── nodes.json                  # All schema nodes
    ├── edges.json                  # Relationships
    └── lineage.json                # Lineage tracking
```

**Extended Metadata Schema:**

```json
{
  "schemaId": 20001,
  "subject": "main.bronze.users",
  "version": 1,
  "context": "unity-catalog",
  "schemaType": "ICEBERG",
  "platform": "databricks",
  "sourceInfo": {
    "platform": "unity-catalog",
    "catalog": "main",
    "schema": "bronze",
    "table": "users",
    "tableFormat": "ICEBERG",
    "location": "s3://bucket/path/to/table",
    "provider": "DATABRICKS",
    "importedAt": "2025-11-13T15:00:00Z",
    "lastModified": "2025-11-10T10:00:00Z"
  },
  "compatibilityMode": "BACKWARD",
  "deploymentTargets": {
    "kafkaTopics": ["user-events-cdc"],
    "unityTables": ["main.bronze.users"],
    "snowflakeTables": ["ANALYTICS_DB.PUBLIC.DIM_USERS"]
  },
  "lineage": {
    "upstreamSources": [
      {
        "type": "kafka-topic",
        "name": "user-events-raw",
        "schemaId": 10001
      }
    ],
    "downstreamTargets": [
      {
        "type": "unity-table",
        "name": "main.silver.users_enriched"
      }
    ],
    "transformations": [
      {
        "type": "spark-job",
        "name": "bronze_to_silver_users",
        "notebook": "notebooks/etl/users.py"
      }
    ]
  },
  "governance": {
    "owner": "data-engineering@company.com",
    "classification": "PII",
    "piiFields": ["email", "phone_number"],
    "retentionDays": 365,
    "approvedBy": "data-governance@company.com",
    "approvalDate": "2025-11-13T14:00:00Z"
  },
  "tags": ["users", "pii", "iceberg", "bronze"],
  "description": "User dimension table imported from Unity Catalog",
  "createdAt": "2025-11-13T15:00:00Z",
  "createdBy": "unity-catalog-importer"
}
```

---

## Schema Metadata Graph

### Graph Database Schema

**Using Neo4j:**

**Node Types:**

```cypher
// Schema Node
CREATE (s:Schema {
  schemaId: 20001,
  subject: "main.bronze.users",
  version: 1,
  platform: "unity-catalog",
  schemaType: "ICEBERG",
  hash: "a1b2c3...",
  createdAt: datetime()
})

// Table Node
CREATE (t:Table {
  fullName: "main.bronze.users",
  catalog: "main",
  schema: "bronze",
  table: "users",
  platform: "unity-catalog",
  tableFormat: "ICEBERG"
})

// Kafka Topic Node
CREATE (k:KafkaTopic {
  name: "user-events-cdc",
  cluster: "us-west-primary"
})

// Transformation Node
CREATE (tr:Transformation {
  name: "bronze_to_silver_users",
  type: "spark-job",
  code: "notebooks/etl/users.py"
})
```

**Edge Types:**

```cypher
// Schema defines Table structure
(s:Schema)-[:DEFINES]->(t:Table)

// Schema is deployed to Topic
(s:Schema)-[:DEPLOYED_TO]->(k:KafkaTopic)

// Lineage: Table reads from Topic
(t:Table)-[:CONSUMES_FROM]->(k:KafkaTopic)

// Lineage: Transformation processes data
(tr:Transformation)-[:READS_FROM]->(t1:Table)
(tr:Transformation)-[:WRITES_TO]->(t2:Table)

// Schema evolution
(s1:Schema {version: 1})-[:EVOLVED_TO]->(s2:Schema {version: 2})

// Schema compatibility
(s1:Schema)-[:COMPATIBLE_WITH]->(s2:Schema)

// Schema references (nested types)
(s1:Schema)-[:REFERENCES]->(s2:Schema)
```

### Graph Query Examples

**Query 1: Find all downstream impacts of schema change**

```cypher
MATCH path = (s:Schema {subject: "user-events-value", version: 2})
             -[:DEPLOYED_TO|DEFINES*1..5]->
             (downstream)
RETURN path
```

**Query 2: Find all PII-containing schemas**

```cypher
MATCH (s:Schema)
WHERE s.classification = "PII"
RETURN s.subject, s.platform, s.piiFields
```

**Query 3: Lineage trace from Kafka to Lakehouse**

```cypher
MATCH path = (k:KafkaTopic {name: "user-events-raw"})
             -[:CONSUMED_BY|WRITES_TO*1..10]->
             (t:Table {platform: "unity-catalog"})
RETURN path
```

**Query 4: Detect schema drift across platforms**

```cypher
MATCH (s1:Schema {subject: "users"})-[:DEPLOYED_TO]->(k:KafkaTopic),
      (s2:Schema {subject: "main.bronze.users"})-[:DEFINES]->(t:Table)
WHERE s1.hash <> s2.hash
RETURN s1, s2, "DRIFT DETECTED" as alert
```

---

## Multi-Source Schema Management

### Import Workflow

```
┌─────────────────┐
│ Unity Catalog   │
│ (Databricks)    │
└────────┬────────┘
         │ 1. Discover Tables
         ▼
┌─────────────────┐
│ UC Importer     │
│ Service         │
└────────┬────────┘
         │ 2. Fetch Iceberg Schema
         ▼
┌─────────────────┐
│ Schema          │
│ Transformer     │
└────────┬────────┘
         │ 3. Convert to Avro
         ▼
┌─────────────────┐
│ Schema ID       │
│ Allocator       │
└────────┬────────┘
         │ 4. Assign Global ID
         ▼
┌─────────────────┐
│ SCHEMASTORE     │
│ (Git)           │
└────────┬────────┘
         │ 5. Commit Schema
         ▼
┌─────────────────┐
│ Metadata Graph  │
│ (Neo4j)         │
└────────┬────────┘
         │ 6. Update Lineage
         ▼
┌─────────────────┐
│ Deployment      │
│ Engine          │
└─────────────────┘
```

### Comparison Engine

**Compare Schemas Across Platforms:**

```python
class SchemaComparator:
    def compare(self, schema1: Schema, schema2: Schema) -> ComparisonResult:
        """
        Compare two schemas and return detailed diff
        """
        return ComparisonResult(
            compatible=self.check_compatibility(schema1, schema2),
            field_diffs=self.compare_fields(schema1, schema2),
            type_mappings=self.map_types(schema1, schema2),
            recommendations=self.generate_recommendations(schema1, schema2)
        )

    def compare_fields(self, schema1, schema2):
        """
        Field-level comparison
        """
        diffs = []

        fields1 = {f.name: f for f in schema1.fields}
        fields2 = {f.name: f for f in schema2.fields}

        # Fields in schema1 but not in schema2
        for name in fields1.keys() - fields2.keys():
            diffs.append(FieldDiff(
                field=name,
                change="REMOVED",
                schema1_type=fields1[name].type,
                schema2_type=None
            ))

        # Fields in schema2 but not in schema1
        for name in fields2.keys() - fields1.keys():
            diffs.append(FieldDiff(
                field=name,
                change="ADDED",
                schema1_type=None,
                schema2_type=fields2[name].type
            ))

        # Fields in both - check type changes
        for name in fields1.keys() & fields2.keys():
            if fields1[name].type != fields2[name].type:
                diffs.append(FieldDiff(
                    field=name,
                    change="TYPE_CHANGED",
                    schema1_type=fields1[name].type,
                    schema2_type=fields2[name].type
                ))

        return diffs
```

**Comparison Report Example:**

```yaml
comparison:
  source1:
    platform: kafka
    subject: user-events-value
    version: 2
  source2:
    platform: unity-catalog
    table: main.bronze.users
    version: 1

compatibility: FORWARD_COMPATIBLE

field_diffs:
  - field: user_id
    change: NONE
    source1_type: long
    source2_type: long
    compatible: true

  - field: email
    change: NULLABILITY_CHANGED
    source1_type: string
    source2_type: string (nullable)
    compatible: true
    impact: LOW

  - field: phone_number
    change: ADDED
    source1_type: null
    source2_type: string (nullable)
    compatible: true
    impact: NONE

  - field: created_date
    change: TYPE_CHANGED
    source1_type: string
    source2_type: timestamp
    compatible: false
    impact: BREAKING
    recommendation: "Add type conversion in consumer: parse string as ISO-8601 timestamp"

summary:
  total_fields: 4
  unchanged: 1
  added: 1
  removed: 0
  type_changed: 1
  breaking_changes: 1

recommendations:
  - "Update Kafka schema to use timestamp logical type instead of string"
  - "Add data transformation layer to convert between formats"
  - "Consider schema evolution strategy for existing data"
```

---

## Architecture Extensions

### New Components

**1. Unity Catalog Client**
- REST API wrapper
- Authentication handling
- Pagination support
- Error handling & retries

**2. Iceberg Schema Parser**
- Parse Iceberg schema JSON
- Extract field definitions
- Handle nested types
- Preserve metadata

**3. Schema Transformer**
- Type mapping engine
- Format conversion (Iceberg → Avro, etc.)
- Validation of transformed schemas
- Bidirectional transformation

**4. Schema Importer Service**
- Scheduled discovery jobs
- Incremental import
- Conflict resolution
- Import status tracking

**5. Metadata Graph Service**
- Neo4j graph database
- Graph query engine
- Lineage tracker
- Visualization API

**6. Comparison Engine**
- Multi-format schema comparison
- Compatibility analysis
- Drift detection
- Migration planning

### Extended API Endpoints

```python
# Unity Catalog Integration
POST /api/v1/unity-catalog/import
  - Import schemas from Unity Catalog

GET /api/v1/unity-catalog/catalogs
  - List available catalogs

GET /api/v1/unity-catalog/tables?catalog={c}&schema={s}
  - List tables in catalog/schema

POST /api/v1/unity-catalog/sync
  - Sync schemas bidirectionally

# Schema Comparison
POST /api/v1/schemas/compare
  - Compare two schemas

GET /api/v1/schemas/drift-report
  - Detect drift across platforms

# Metadata Graph
GET /api/v1/graph/lineage?subject={subject}
  - Get schema lineage

GET /api/v1/graph/downstream?subject={subject}
  - Find downstream dependencies

POST /api/v1/graph/query
  - Execute Cypher query

# Discovery
POST /api/v1/discovery/scan?platform={platform}
  - Run discovery scan

GET /api/v1/discovery/status/{job_id}
  - Get discovery job status
```

---

## Implementation Plan

### Phase 1: Unity Catalog Foundation (Weeks 1-2)

**Deliverables:**
- Unity Catalog REST client
- Iceberg schema parser
- Type mapping engine
- Basic import functionality

**Tasks:**
1. Implement UC authentication
2. Build REST API client
3. Parse Iceberg schema format
4. Map Iceberg types to Avro
5. Store imported schemas in SCHEMASTORE

### Phase 2: Schema Comparison (Weeks 3-4)

**Deliverables:**
- Schema comparison engine
- Compatibility analyzer
- Drift detection
- Comparison reports

**Tasks:**
1. Implement field-level comparison
2. Build compatibility checker
3. Create diff visualization
4. Generate migration recommendations

### Phase 3: Metadata Graph (Weeks 5-6)

**Deliverables:**
- Neo4j integration
- Graph data model
- Lineage tracking
- Query API

**Tasks:**
1. Set up Neo4j database
2. Define graph schema
3. Build lineage tracker
4. Implement graph queries
5. Create visualization endpoints

### Phase 4: Discovery & Automation (Weeks 7-8)

**Deliverables:**
- Scheduled discovery
- Incremental sync
- Event-driven updates
- Automated drift alerts

**Tasks:**
1. Build discovery scheduler
2. Implement change detection
3. Add webhook support
4. Create alert system

### Phase 5: Web UI Extensions (Weeks 9-10)

**Deliverables:**
- Platform selector
- Comparison view
- Lineage graph visualization
- Import wizard

**Tasks:**
1. Add Unity Catalog connection UI
2. Build schema comparison page
3. Integrate graph visualization library
4. Create import wizard flow

---

## Enterprise Benefits

### For Data Engineers
- Single source of truth for all schemas
- Automated schema discovery
- Cross-platform compatibility checking
- Reduced manual schema management

### For Data Architects
- Complete schema lineage visibility
- Impact analysis for changes
- Governance enforcement
- Architecture insights

### For Platform Teams
- Simplified multi-platform operations
- Consistent schema management
- Automated compliance checking
- Reduced operational overhead

### For Business
- Faster time to insights
- Reduced data quality issues
- Better regulatory compliance
- Lower TCO for data infrastructure

---

**Document Version:** 2.0 Enterprise
**Last Updated:** 2025-11-13
**Owner:** Platform Engineering Team - Enterprise Division
