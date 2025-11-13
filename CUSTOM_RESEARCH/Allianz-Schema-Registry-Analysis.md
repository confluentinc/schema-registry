# Allianz Project: Schema Registry Management Analysis

**Status:** Draft
**Date:** 2025-11-13
**Project:** Allianz Confluent Cloud Migration
**Focus:** Overcoming Schema Registry limitations and enabling workload mobility

---

## Executive Summary

This document analyzes the challenges and solutions for managing Confluent Schema Registries in a multi-environment Confluent Cloud deployment for the Allianz project. The primary challenge is overcoming the 20,000 schema limit per Schema Registry while enabling workload mobility and high availability across cloud environments.

**Key Challenge:** Confluent Cloud environments have isolated Schema Registries that are not interconnected, limiting workload portability and creating potential scalability bottlenecks.

**Key Solution Approaches:**
1. Independent/Isolated per environment
2. Schema Linking for selective replication
3. Global Schema ID coordination with external injection

---

## 1. Current Status at Customer

**[To be completed with customer-specific information]**

### Current Environment
- Migration to Confluent Cloud in progress (2025)
- Multiple cloud environments deployed
- Each environment has its own isolated Schema Registry
- No current interconnection between Schema Registries across environments

### Key Constraints Identified
- 20,000 schema limit per Schema Registry
- Schema Registries are not connected across Confluent Cloud environments
- Each environment can only use its locally connected Schema Registry
- Limited workload mobility between environments

---

## 2. Goal: Overcome the Limits of 20k Schemas per Schema Registry

### Problem Statement

The Confluent Schema Registry has a practical limit of approximately 20,000 schemas per registry instance. In enterprise environments with:
- Multiple business domains
- Hundreds of services and applications
- Long-term schema evolution
- Multi-environment deployments (dev, test, staging, production)

This limit becomes a significant bottleneck.

### Business Impact

**Without a solution:**
- **Scale limitations:** Cannot support continued growth of data products
- **Workload immobility:** Applications tied to specific environments
- **HA challenges:** Difficult to implement cross-environment failover
- **Operational complexity:** Manual schema synchronization required
- **Risk:** Service disruptions during environment failures

### Success Criteria

A successful solution must provide:
1. **Scalability:** Support for >20,000 schemas globally
2. **Workload Mobility:** Producers can move between environments
3. **High Availability:** Automatic failover across environments
4. **Consistency:** Global unique Schema IDs across all environments
5. **Selective Distribution:** Only relevant schemas in each environment
6. **Automation:** CI/CD-driven schema deployment

---

## 3. Options for Schema Management Procedures

### 3.1 Independent / Isolated Schema Registries

**Description:** Each Confluent Cloud environment maintains its own independent Schema Registry with no coordination or synchronization.

#### Architecture

```
Environment A          Environment B          Environment C
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│   SR-A      │       │   SR-B      │       │   SR-C      │
│  (Isolated) │       │  (Isolated) │       │  (Isolated) │
│             │       │             │       │             │
│ ID: 1-5000  │       │ ID: 1-4000  │       │ ID: 1-6000  │
└─────────────┘       └─────────────┘       └─────────────┘
      ↓                     ↓                     ↓
  Workload-A            Workload-B            Workload-C
```

#### Characteristics

**Advantages:**
- Simple operational model
- No coordination overhead
- Environment isolation reduces blast radius
- Each environment stays within 20k limit
- No cross-environment dependencies

**Disadvantages:**
- Schema ID collisions inevitable
- No workload mobility between environments
- Duplicate schema definitions across environments
- Manual synchronization required
- No global schema governance
- Data incompatibility when crossing environment boundaries

#### Use Cases

Best suited for:
- Completely isolated environments (air-gapped)
- Development/testing environments with synthetic data
- Short-term tactical solutions
- Proof-of-concept deployments

**Recommendation:** ❌ Not suitable for Allianz production requirements

---

### 3.2 Schema Linking with Contexts

**Description:** Use Confluent's Schema Linking feature to replicate schemas across Schema Registry boundaries while preserving Schema IDs. Leverage contexts for namespace isolation.

#### Architecture Patterns

##### Pattern A: Pairwise Failover

```
    Primary                    Secondary
┌─────────────┐           ┌─────────────┐
│   SR-A      │  Schema   │   SR-B      │
│             │  Linking  │             │
│ Schemas:    │ ───────>  │ Schemas:    │
│ 1,2,3,4,5   │           │ 1,2,3,4,5   │
│             │           │ (replicated)│
└─────────────┘           └─────────────┘
      ↓                         ↓
  Workload-A    Failover    Workload-A
              ─────────────>
```

##### Pattern B: Multi-directional Linking

```
        SR-A
         ↓↑
        ╱  ╲
       ╱    ╲
      ↓      ↑
   SR-B ←→ SR-C

   (Triangle configuration)
```

##### Pattern C: Hub-and-Spoke with Contexts

```
          Central SR (Hub)
        Context: global
              ╱  |  ╲
    Schema   ╱   |   ╲   Schema
    Linking ╱    |    ╲  Linking
           ↓     ↓     ↓
        SR-A   SR-B   SR-C
     Context: Context: Context:
       us-west eu-west ap-south
```

#### Schema Linking Features

**What Schema Linking Provides:**
- Replication of schema definitions across registries
- Preservation of Schema IDs during replication
- Automatic synchronization of schema updates
- Support for selective subject replication
- Compatibility with Cluster Linking

**Configuration Example:**

```bash
# Create Schema Link from SR-A to SR-B
confluent schema-registry exporter create \
  --name sr-a-to-sr-b \
  --subjects "topic-.*-value" \
  --context-type CUSTOM \
  --source-sr http://sr-a:8081 \
  --destination-sr http://sr-b:8081
```

#### Context Strategy

**Context Isolation:**
- Each environment operates in a dedicated context
- Context provides namespace isolation
- Allows same subject names across environments
- Schema IDs unique within context scope

**Global Context:**
- Shared schemas use global context
- Common data models accessible everywhere
- Reduces duplication for enterprise-wide schemas

**Example Context Structure:**
```
contexts:
  - global          # Enterprise-wide schemas
  - us-prod         # US Production
  - eu-prod         # EU Production
  - ap-prod         # APAC Production
  - dev             # Development
```

#### Workload Mobility Scenarios

##### Scenario 1: Regional Failover
```
Normal Operation:
  Producer-A → SR-US (ID: 100) → Topic-US

After Failover:
  Producer-A → SR-EU (ID: 100) → Topic-EU
                      ↑
                (Schema Linked, same ID preserved)
```

##### Scenario 2: Cross-Region Active-Active
```
Region US:
  SR-US ←→ Schema Linking ←→ SR-EU
  ↓                            ↓
  Producer-US              Producer-EU
  (can write to either region)
```

#### Implementation Considerations

**Schema Linking Configuration:**
1. Determine replication topology (pairwise, hub-spoke, mesh)
2. Define which subjects/schemas to replicate
3. Set up monitoring for replication lag
4. Configure context boundaries

**Limitations:**
- Still subject to 20k limit per registry (replication doesn't increase capacity)
- Replication lag during high-volume schema updates
- Requires Schema Linking license (Confluent Enterprise)
- Complexity increases with number of environments

**Advantages:**
- ✅ Native Confluent feature
- ✅ Schema ID consistency guaranteed
- ✅ Automatic synchronization
- ✅ Supports workload mobility for linked schemas
- ✅ Works seamlessly with Cluster Linking

**Disadvantages:**
- ⚠️ Replicates all matched schemas (potential over-replication)
- ⚠️ Doesn't solve 20k limit fundamentally
- ⚠️ Licensing costs
- ⚠️ Operational complexity in mesh topologies

**Recommendation:** ✅ Good solution for HA and failover, but combine with approach 3.3 for scale

---

### 3.3 Explicit External Injection with Region Focus and Consistency

**Description:** Central schema management system allocates global unique Schema IDs and selectively deploys schemas to relevant regions. Each regional Schema Registry operates in IMPORT mode to accept externally managed IDs.

#### Architecture

```
┌─────────────────────────────────────────┐
│   Global Schema ID Coordinator          │
│   (Central Authority)                    │
│                                          │
│   • Assigns globally unique Schema IDs  │
│   • Maintains global schema catalog     │
│   • Tracks schema → region mappings     │
│   • Provides ID allocation API          │
└───────────────┬─────────────────────────┘
                │
                │ CI/CD Pipeline
                │ (Schema Deployment)
                ▼
    ┌───────────┴───────────┬────────────┐
    │                       │            │
    ▼                       ▼            ▼
┌─────────┐           ┌─────────┐   ┌─────────┐
│  SR-US  │           │  SR-EU  │   │  SR-AP  │
│ (IMPORT)│           │ (IMPORT)│   │ (IMPORT)│
│         │           │         │   │         │
│ Schemas:│           │ Schemas:│   │ Schemas:│
│ 1,5,7   │           │ 2,3,8   │   │ 4,6,9   │
│ 100-150 │           │ 2,8,101│   │ 5,105   │
│         │           │         │   │         │
│ ~3k IDs │           │ ~2.5k   │   │ ~1.8k   │
└─────────┘           └─────────┘   └─────────┘
     ↓                     ↓             ↓
 Workload-US          Workload-EU   Workload-AP
 (uses IDs:           (uses IDs:    (uses IDs:
  1,5,7,100-150)       2,3,8,101)    4,6,9,105)
```

#### How It Works

##### Phase 1: Global Schema ID Allocation

**Central Coordinator Service:**
- Maintains authoritative global schema catalog
- Assigns unique IDs across all regions
- Prevents ID collisions globally
- Tracks schema deployment targets

**API Example:**
```json
POST /coordinator/schema/register
{
  "schema": "{\"type\":\"record\",\"name\":\"Order\",...}",
  "schemaType": "AVRO",
  "targetRegions": ["us-west", "eu-central"],
  "owningService": "order-service"
}

Response:
{
  "id": 10042,
  "existing": false,
  "deploymentPlan": {
    "us-west": "pending",
    "eu-central": "pending"
  }
}
```

##### Phase 2: Selective Regional Deployment

**Deployment Strategy:**
- Only deploy schemas to regions where workloads need them
- CI/CD pipeline orchestrates deployment
- Schema ID gaps are acceptable and expected
- Each region optimized for local workloads

**Deployment Example:**
```yaml
# schema-deployment.yaml
schemas:
  - name: OrderCreated
    file: schemas/order-created.avsc
    regions:
      - us-west-2
      - eu-central-1
    services:
      - order-processor
      - order-analytics

  - name: PaymentProcessed
    file: schemas/payment-processed.avsc
    regions:
      - us-west-2  # Payment processing only in US
    services:
      - payment-service
```

**CI/CD Workflow:**
```bash
# 1. Register with coordinator (gets global ID)
SCHEMA_ID=$(curl -X POST $COORDINATOR_URL/schema/register \
  -d @order-created.json | jq -r '.id')

# 2. Deploy to target regions with assigned ID
for region in us-west eu-central; do
  curl -X POST https://${region}-sr.confluent.cloud/subjects/OrderCreated-value/versions \
    -u $API_KEY:$API_SECRET \
    -H "Content-Type: application/json" \
    -d "{
      \"schema\": \"$(cat order-created.avsc)\",
      \"schemaType\": \"AVRO\",
      \"id\": $SCHEMA_ID
    }"
done
```

##### Phase 3: IMPORT Mode Configuration

**Enable Schema Registry IMPORT Mode:**

Each regional Schema Registry must be configured to accept external IDs:

```bash
# Configure SR to allow custom IDs
curl -X PUT https://sr-us-west.confluent.cloud/mode \
  -u $API_KEY:$API_SECRET \
  -H "Content-Type: application/json" \
  -d '{"mode": "IMPORT"}'
```

**IMPORT Mode Characteristics:**
- Accepts externally provided Schema IDs
- Skips ID auto-generation
- Bypasses compatibility checks (validation done externally)
- Allows ID gaps
- Idempotent: same schema + same ID = success

#### Region Focus Strategy

**Workload-to-Region Mapping:**

```
Service Categories → Target Regions
────────────────────────────────────
Order Management    → US, EU, AP (global)
Payment Processing  → US only (compliance)
EU Customer Data    → EU only (GDPR)
Analytics           → US, EU (aggregation hubs)
Logging             → All regions (local only)
```

**Schema Distribution Matrix:**

| Schema ID | Schema Name | US | EU | AP | Services |
|-----------|-------------|----|----|----|---------|
| 1001 | OrderCreated | ✅ | ✅ | ✅ | order-service |
| 1002 | PaymentProcessed | ✅ | ❌ | ❌ | payment-service |
| 1003 | CustomerProfile | ❌ | ✅ | ❌ | customer-service |
| 1004 | AnalyticsEvent | ✅ | ✅ | ❌ | analytics-platform |

**Benefits of Region Focus:**
- Each region stays well under 20k limit
- Faster schema lookups (smaller registry)
- Reduced memory footprint
- Optimized for local workload needs
- Global capacity = regions × ~20k

#### Consistency Guarantees

**Global Uniqueness:**
- Central coordinator ensures no ID collisions
- Schema ID → Schema Content mapping is 1:1 globally
- Same ID always means same schema everywhere

**Acceptable Gaps:**
- Region US might have IDs: 1, 5, 7, 100-150 (gaps at 2,3,4,6,8-99)
- Region EU might have IDs: 2, 3, 8, 101-200
- No conflict because 2, 3, 8 in EU are same globally

**Data Portability:**
```
Producer in US writes message with Schema ID 1001
   ↓
Message sent to Kafka
   ↓
Cluster Linking replicates to EU
   ↓
Consumer in EU reads message with Schema ID 1001
   ↓
EU Schema Registry has ID 1001 (pre-deployed)
   ↓
Successful deserialization
```

#### Workload Mobility with External Injection

##### Onboarding Process for Workload Startup

**Scenario:** Producer application starts in a new environment

```bash
#!/bin/bash
# workload-onboarding.sh

# 1. Query required schemas from coordinator
REQUIRED_SCHEMAS=$(curl $COORDINATOR_URL/schemas/for-service/order-producer)

# 2. Check which schemas missing in local SR
for schema_id in $REQUIRED_SCHEMAS; do
  if ! schema_exists_locally $schema_id; then
    # 3. Fetch from coordinator
    SCHEMA_DEF=$(curl $COORDINATOR_URL/schemas/ids/$schema_id)

    # 4. Register in local SR with same ID
    register_schema_locally "$SCHEMA_DEF" "$schema_id"
  fi
done

# 5. Start application
exec java -jar order-producer.jar
```

**Pull-based Schema Activation:**
- Application declares required schemas in manifest
- Init container pulls from global coordinator
- Registers in local SR before app starts
- Ensures Schema ID consistency
- Enables ad-hoc workload placement

##### Producer Workload Movement

**Why focus on Producers?**

**Producer Challenge:**
- Must register schema before writing
- Schema ID written into message headers
- Requires Schema Registry availability
- Schema must exist with correct ID

**Consumer Advantage:**
- Schema already registered by producer
- Read-only operation
- No registration needed
- Schema ID in message guides lookup

**Conclusion: Schema Deployment = Producer Deployment**

**Producer Movement Example:**

```
Before:
  US Environment
    SR-US (has schema ID 5001)
    Producer-A (writes with ID 5001)

After Failure:
  EU Environment
    SR-EU (needs schema ID 5001)
    Producer-A failover

Solution:
  1. Detect producer failover needed
  2. Ensure schema 5001 exists in SR-EU
     - Option A: Pre-deployed via CI/CD
     - Option B: On-demand pull from coordinator
  3. Start Producer-A in EU
  4. Producer writes with same ID 5001
```

#### Implementation Components

##### Central Coordinator Service

**Technology Options:**
- Custom service (Spring Boot, Go)
- Schema Registry extension
- API Gateway with backing database

**Core Functions:**
```
API Endpoints:
  POST   /schemas/register          → Allocate new ID
  GET    /schemas/{id}              → Retrieve schema by ID
  GET    /schemas/for-service/{svc} → Get required schemas
  POST   /schemas/deploy            → Trigger deployment
  GET    /schemas/conflicts         → Detect ID conflicts

Storage:
  - Global schema catalog (PostgreSQL, DynamoDB)
  - Schema content + metadata
  - Deployment tracking
  - Audit log
```

##### CI/CD Integration

**GitOps Workflow:**

```yaml
# schemas/order-created/schema.avsc
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.allianz.orders",
  "fields": [...]
}

# schemas/order-created/deployment.yaml
apiVersion: schema.allianz.com/v1
kind: SchemaDeployment
metadata:
  name: order-created
spec:
  schemaFile: schema.avsc
  schemaType: AVRO
  targetRegions:
    - us-west-2
    - eu-central-1
  services:
    - order-service
    - order-processor
```

**Pipeline Steps:**
1. Developer commits schema change
2. CI validates schema (compatibility, standards)
3. CI requests ID from coordinator
4. CI deploys to target regions
5. CD updates service configurations
6. Service restart with new schema

##### Monitoring and Validation

**Health Checks:**
- Schema ID uniqueness validator (runs hourly)
- Cross-region consistency checker
- Missing schema detector (schema in coordinator but not deployed)
- Deployment lag monitor

**Alerts:**
- ID collision detected
- Schema registration failed
- Coordinator unreachable
- Schema deployment drift

**Dashboards:**
- Schemas per region
- ID allocation rate
- Deployment success rate
- Workload schema dependencies

#### Advantages

✅ **Scalability:** Global capacity = regions × 20k
✅ **Consistency:** Global unique Schema IDs guaranteed
✅ **Optimization:** Only relevant schemas per region
✅ **Automation:** CI/CD-driven deployment
✅ **Workload Mobility:** Producers can move with schema preloading
✅ **Governance:** Central visibility and control
✅ **Gap Tolerance:** IDs don't need to be sequential
✅ **Native Support:** Uses IMPORT mode (built-in feature)

#### Disadvantages

⚠️ **Complexity:** Requires coordinator service implementation
⚠️ **Dependency:** Coordinator is critical infrastructure
⚠️ **Operational Overhead:** More components to manage
⚠️ **Initial Setup:** CI/CD pipeline changes required
⚠️ **Mode Constraint:** IMPORT mode skips compatibility checks

#### Mitigations

**Coordinator Availability:**
- Deploy coordinator with HA (multi-AZ)
- Use managed database for storage
- Implement caching for schema lookups
- Allow graceful degradation (local fallback)

**Compatibility Validation:**
- Run compatibility checks in CI before coordinator
- Use Confluent Schema Registry libraries for validation
- Fail pipeline if incompatible change detected

**Documentation:**
- Comprehensive runbooks
- Architecture diagrams
- Troubleshooting guides
- Training for dev and ops teams

**Recommendation:** ✅ Best solution for scale + consistency + workload mobility

---

## 4. Failure Scenarios

### 4.1 Failover

**Definition:** Automatic or manual transfer of workload from a failed environment to a healthy environment with minimal disruption.

#### Scenario A: Single Producer Failover

**Context:**
- Producer-A running in US environment
- Writes to topic "orders" with schema ID 1001
- US Kafka cluster experiences outage

**Preconditions for Successful Failover:**
1. ✅ Schema ID 1001 exists in EU Schema Registry
2. ✅ Topic "orders" exists in EU (via Cluster Linking or pre-created)
3. ✅ EU Kafka cluster operational
4. ✅ Network connectivity from producer to EU

**Failover Sequence:**

```
Time  Event
────  ─────────────────────────────────────────
T+0   US Kafka cluster fails
T+1   Health check detects failure
T+2   Orchestrator triggers failover
T+3   Verify schema 1001 in SR-EU (✅ pre-linked)
T+4   Update Producer-A config:
      bootstrap.servers: kafka-eu.confluent.cloud
      schema.registry.url: https://sr-eu.confluent.cloud
T+5   Restart Producer-A
T+6   Producer-A registers/verifies schema (idempotent)
T+7   Producer-A writes to EU Kafka
T+8   Consumers in EU receive messages
```

**Schema Linking Role:**
- Ensures schema 1001 replicated to SR-EU before failure
- Preserves Schema ID during replication
- Continuous background sync (no manual intervention)

**Cluster Linking Role:**
- Replicates topic "orders" from US to EU
- Maintains consumer offsets
- Preserves message ordering
- Enables seamless consumer continuity

**Configuration:**

```bash
# Schema Linking (US → EU)
confluent schema-registry exporter create \
  --name us-to-eu-schemas \
  --subjects ".*" \
  --source-sr https://sr-us.confluent.cloud \
  --destination-sr https://sr-eu.confluent.cloud

# Cluster Linking (US → EU)
confluent kafka link create us-to-eu \
  --source-cluster us-kafka \
  --destination-cluster eu-kafka \
  --config-file cluster-link.properties

confluent kafka mirror create orders \
  --link us-to-eu \
  --source-topic orders
```

#### Scenario B: Regional Outage with Active-Active

**Context:**
- Active-Active deployment across US and EU
- Bidirectional Cluster Linking and Schema Linking
- Producers in both regions

**Normal Operation:**
```
US Region:                    EU Region:
  Producer-US                   Producer-EU
       ↓                             ↓
  Kafka-US   ←─ Cluster Link ─→  Kafka-EU
       ↓                             ↓
   SR-US     ←─ Schema Link ──→    SR-EU
```

**During US Outage:**
```
US Region: ❌ OFFLINE          EU Region: ✅ ONLINE
  Producer-US (failed)           Producer-EU (continues)
                                  + Producer-US (failed over)
```

**Failover Steps:**
1. Detect US region failure
2. Producer-US instances restart in EU
3. All writes go to EU Kafka
4. Consumers continue (already reading from both regions)
5. When US recovers, Cluster Linking syncs missed data

**Data Consistency:**
- Messages written during outage buffered or lost (depends on producer config)
- After recovery, Cluster Linking replicates EU → US
- Schema Linking ensures schema updates propagated

#### Scenario C: Schema Registry Isolated Failure

**Context:**
- Kafka cluster operational
- Schema Registry fails or becomes unreachable

**Impact Analysis:**

**Producers:**
- ❌ Cannot register new schemas
- ✅ Can write if schema cached locally
- ⚠️ Fail if schema cache miss

**Consumers:**
- ✅ Can read if schema cached locally
- ❌ Fail on schema cache miss
- ⚠️ New consumer instances cannot start

**Mitigation:**

```properties
# Producer config: Aggressive caching
schema.registry.cache.capacity=1000
schema.registry.cache.ttl.sec=86400

# Consumer config
schema.registry.cache.capacity=1000
```

**Failover to Secondary SR:**

```java
// Application configuration
List<String> schemaRegistryUrls = Arrays.asList(
  "https://sr-us-primary.confluent.cloud",
  "https://sr-us-secondary.confluent.cloud",
  "https://sr-eu-backup.confluent.cloud"
);

Properties props = new Properties();
props.put("schema.registry.url", String.join(",", schemaRegistryUrls));
```

**External Injection Advantage:**
- If coordinator available, can deploy schema to alternate SR
- Producer pulls from coordinator, registers in fallback SR
- More flexible than pure Schema Linking approach

---

### 4.2 Migrate Workload

**Definition:** Planned relocation of a workload from one environment to another (not due to failure).

#### Use Cases

1. **Cost Optimization:** Move workload to cheaper region
2. **Data Locality:** Co-locate processing with data sources
3. **Regulatory Compliance:** Move to compliant region (e.g., GDPR)
4. **Load Balancing:** Distribute across regions
5. **Maintenance:** Evacuate environment for upgrades

#### Migration Strategies

##### Strategy 1: Blue-Green Migration

**Approach:** Run workload in both environments, then switch traffic

```
Phase 1: Preparation
  - Deploy schema to target region (Green)
  - Set up Cluster Linking US → EU
  - Deploy application in EU (not started)

Phase 2: Parallel Operation
  - Start Producer-EU (writes to EU Kafka)
  - Producer-US still running (writes to US Kafka)
  - Both environments active

Phase 3: Traffic Switch
  - Update load balancer / service mesh
  - Route 100% traffic to EU
  - Monitor for 24 hours

Phase 4: Decommission
  - Stop Producer-US
  - Archive US data
  - Clean up US resources
```

**Schema Requirements:**
- Schema ID must exist in target region before migration
- Use approach 3.3 (external injection) to ensure consistency
- Validate schema availability in target SR

##### Strategy 2: Rolling Migration

**Approach:** Gradually shift traffic instance by instance

```
Initial State:
  US: 10 producer instances
  EU: 0 producer instances

Step 1:
  US: 8 instances
  EU: 2 instances (20% traffic)

Step 2:
  US: 5 instances
  EU: 5 instances (50% traffic)

Step 3:
  US: 2 instances
  EU: 8 instances (80% traffic)

Final:
  US: 0 instances
  EU: 10 instances (100% traffic)
```

**Benefits:**
- Gradual validation
- Easy rollback
- Lower risk

##### Strategy 3: Read Replica Promotion

**Approach:** Convert a read-only replica to primary

```
Before:
  US (Primary):
    - Producers write here
    - Cluster Link → EU
    - Schema Link → EU

  EU (Replica):
    - Read-only mirrors
    - Consumers only

Migration:
  1. Pause US producers
  2. Wait for replication lag = 0
  3. Promote EU to primary
  4. Start producers in EU
  5. Reverse Cluster Link direction (EU → US)
```

#### Producer Migration Workflow

**Step-by-Step Process:**

```bash
#!/bin/bash
# migrate-producer.sh

SOURCE_REGION="us-west-2"
TARGET_REGION="eu-central-1"
SERVICE="order-producer"

echo "=== Step 1: Validate schemas in target region ==="
# Get schemas used by service
SCHEMAS=$(curl $COORDINATOR_URL/schemas/for-service/$SERVICE)

# Check each schema exists in target SR
for schema_id in $SCHEMAS; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    https://sr-${TARGET_REGION}.confluent.cloud/schemas/ids/$schema_id)

  if [ $STATUS != "200" ]; then
    echo "ERROR: Schema $schema_id not in target SR"
    echo "Deploying schema $schema_id to $TARGET_REGION..."
    deploy_schema $schema_id $TARGET_REGION
  fi
done

echo "=== Step 2: Verify Cluster Linking ==="
# Ensure topics replicated
TOPICS=$(get_producer_topics $SERVICE)
for topic in $TOPICS; do
  verify_cluster_link $SOURCE_REGION $TARGET_REGION $topic
done

echo "=== Step 3: Deploy application in target ==="
kubectl apply -f k8s/producer-${TARGET_REGION}.yaml

echo "=== Step 4: Update configuration ==="
kubectl set env deployment/$SERVICE \
  -n $TARGET_REGION \
  KAFKA_BOOTSTRAP_SERVERS=kafka-${TARGET_REGION}.confluent.cloud:9092 \
  SCHEMA_REGISTRY_URL=https://sr-${TARGET_REGION}.confluent.cloud

echo "=== Step 5: Start traffic ==="
kubectl scale deployment/$SERVICE --replicas=3 -n $TARGET_REGION

echo "=== Step 6: Monitor ==="
monitor_producer_health $SERVICE $TARGET_REGION

echo "=== Step 7: Drain source region ==="
kubectl scale deployment/$SERVICE --replicas=0 -n $SOURCE_REGION

echo "Migration complete!"
```

#### Consumer Migration (Simpler)

**Why simpler?**
- Consumers don't register schemas
- Read-only operations
- No Schema ID concerns

**Process:**
```bash
# 1. Deploy consumer in target region
kubectl apply -f consumer-eu.yaml

# 2. Consumer automatically reads schemas by ID from SR-EU
#    (schemas already replicated via Schema Linking)

# 3. Start consuming
kubectl scale deployment/consumer --replicas=5 -n eu

# 4. Drain source
kubectl scale deployment/consumer --replicas=0 -n us
```

#### Data Migration Considerations

**When Cluster Linking is Active:**
- Data automatically replicated
- Minimal producer downtime
- Consumers can read from either region

**Without Cluster Linking:**
- Must replay data from source to target
- Use Kafka Connect or custom replay tool
- Ensure Schema IDs consistent during replay

**Offset Management:**
- Export consumer group offsets from source
- Import to target with offset translation
- Use `kafka-consumer-groups.sh` or API

---

### 4.3 Recover from Error and Migrate Back

**Definition:** Return workload to original environment after resolving the issue that caused failover.

#### Scenario: Failed Over to EU, Now Returning to US

**Timeline:**

```
T+0   US Kafka outage → Failover to EU
T+1h  Workload running in EU
T+4h  US issue resolved, cluster healthy
T+5h  Decision: Migrate back to US
```

#### Challenges of Migration Back

**Challenge 1: Data Written During Outage**

```
Before Outage:
  US Kafka: Messages 1-1000
  EU Kafka: Messages 1-1000 (replicated)

During Outage (US down):
  EU Kafka: Messages 1001-1500 (new)
  US Kafka: Messages 1-1000 (stale)

Must sync before returning:
  US Kafka needs messages 1001-1500
```

**Solution: Reverse Cluster Linking**

```bash
# Temporarily reverse link direction
confluent kafka link create eu-to-us \
  --source-cluster eu-kafka \
  --destination-cluster us-kafka

# Wait for sync
confluent kafka link lag eu-to-us --topic orders
# Output: Lag: 0 messages

# Proceed with migration back
```

**Challenge 2: Schema Evolution During Failover**

```
Scenario:
  - Schema ID 1001 (v1) existed before outage
  - During outage, developer updated to v2
  - New Schema ID 1002 registered in SR-EU
  - SR-US doesn't have ID 1002

Risk:
  - Producer migrates back to US
  - Attempts to write with schema 1002
  - US SR doesn't recognize it
  - Messages fail to serialize
```

**Solution Options:**

**Option A: Schema Linking (Automatic)**
- If Schema Linking active EU → US
- Schema 1002 automatically synced back
- No manual intervention

**Option B: External Injection (Manual/Automated)**
- CI/CD detects new schema 1002 in EU
- Deploys to US via coordinator
- Ensures ID consistency

**Option C: Pre-Migration Validation**
```bash
# Script to check schema sync before migration
#!/bin/bash
echo "Checking schema consistency..."

SCHEMAS_EU=$(curl https://sr-eu/schemas/ids | jq -r '.[].id')
for id in $SCHEMAS_EU; do
  if ! curl -s https://sr-us/schemas/ids/$id > /dev/null; then
    echo "WARNING: Schema $id in EU but not in US"
    echo "Syncing schema $id..."
    sync_schema $id "eu" "us"
  fi
done

echo "Schema sync complete. Safe to migrate back."
```

#### Migration Back Workflow

##### Phase 1: Preparation

```bash
# 1. Verify US cluster health
kafka-broker-health-check --cluster us-kafka

# 2. Establish reverse Cluster Link (if not already bidirectional)
confluent kafka link create eu-to-us-reverse \
  --source eu-kafka --destination us-kafka

# 3. Sync data (wait for lag = 0)
wait_for_replication_complete eu-kafka us-kafka

# 4. Sync schemas
sync_all_schemas eu-sr us-sr

# 5. Verify Schema Registry consistency
validate_schema_consistency us-sr eu-sr
```

##### Phase 2: Test Migration

```bash
# 1. Deploy test producer in US
kubectl apply -f test-producer-us.yaml

# 2. Send test messages
kubectl exec test-producer -n us -- \
  /opt/kafka/bin/kafka-avro-console-producer \
  --broker-list kafka-us:9092 \
  --topic test-migration \
  --property schema.registry.url=https://sr-us.confluent.cloud \
  --property value.schema='{"type":"record",...}'

# 3. Verify consumption
kubectl exec test-consumer -n us -- consume-and-verify

# 4. If successful, proceed
```

##### Phase 3: Execute Migration

**Blue-Green Approach:**

```
State 1: Running in EU
  EU: Producer active (100% traffic)
  US: Producer staged (0% traffic)

State 2: Dual Active
  EU: Producer active (50% traffic)
  US: Producer active (50% traffic)

State 3: Running in US
  EU: Producer draining (0% traffic)
  US: Producer active (100% traffic)
```

**Execution:**

```bash
# 1. Scale up US producers
kubectl scale deployment/order-producer --replicas=5 -n us

# 2. Update load balancer to split traffic
update_traffic_split us=50 eu=50

# 3. Monitor for 15 minutes
watch_producer_metrics us eu

# 4. If healthy, complete migration
update_traffic_split us=100 eu=0

# 5. Scale down EU
kubectl scale deployment/order-producer --replicas=0 -n eu

# 6. Clean up reverse link (optional)
confluent kafka link delete eu-to-us-reverse
```

##### Phase 4: Validation and Cleanup

```bash
# 1. Verify data flow
verify_end_to_end_flow --region us

# 2. Check consumer lag
kafka-consumer-groups --bootstrap-server kafka-us:9092 \
  --describe --group order-consumers

# 3. Validate schema registry metrics
curl https://sr-us/metrics | grep registration_success_rate

# 4. Archive EU logs
archive_logs eu order-producer $(date +%Y%m%d)

# 5. Update documentation
update_runbook "Producer migrated back to US on $(date)"
```

#### Rollback Plan

**If migration back fails:**

```bash
# Emergency rollback to EU
#!/bin/bash
echo "ROLLBACK INITIATED"

# 1. Stop US producers immediately
kubectl scale deployment/order-producer --replicas=0 -n us

# 2. Restore EU producers
kubectl scale deployment/order-producer --replicas=5 -n eu

# 3. Update traffic routing
update_traffic_split us=0 eu=100

# 4. Verify EU health
verify_producer_health eu

# 5. Incident postmortem
create_incident_report "migration-back-failure-$(date +%s)"
```

#### Common Pitfalls and Solutions

**Pitfall 1: Data Loss During Migration**

**Cause:** Stopping producer before messages flushed

**Solution:**
```java
// Producer shutdown hook
producer.flush();  // Wait for all buffered messages
producer.close(Duration.ofSeconds(30));  // Graceful close
```

**Pitfall 2: Consumer Offset Mismatch**

**Cause:** Offset values differ between US and EU clusters

**Solution:**
- Use Cluster Linking (preserves offsets)
- Or use timestamps for offset reset
```bash
kafka-consumer-groups --reset-offsets \
  --to-datetime 2025-11-13T14:30:00.000 \
  --topic orders --group order-consumers --execute
```

**Pitfall 3: Schema Version Conflict**

**Cause:** Different schema versions between regions

**Solution:**
- Always sync schemas before data migration
- Use external coordinator for source of truth
- Validate schema compatibility in CI/CD

**Pitfall 4: Incomplete Cluster Linking Sync**

**Cause:** Starting migration before replication complete

**Solution:**
```bash
# Wait function
wait_for_zero_lag() {
  while true; do
    LAG=$(confluent kafka link lag $LINK_NAME --topic $TOPIC | grep -oP '\d+')
    if [ "$LAG" -eq 0 ]; then
      echo "Replication complete"
      break
    fi
    echo "Lag: $LAG messages, waiting..."
    sleep 10
  done
}
```

---

## 5. Recommended Solution Architecture

### Combined Approach: External Injection + Schema Linking

**Rationale:**
- External injection provides global scale and consistency
- Schema Linking provides HA and failover capability
- Together they solve all key requirements

### Architecture Diagram

```
                    ┌─────────────────────────────┐
                    │  Global Schema Coordinator  │
                    │  (Source of Truth)          │
                    │  - Assigns Global IDs       │
                    └─────────────┬───────────────┘
                                  │
                                  │ CI/CD Deployment
                                  ▼
                    ┌─────────────────────────────┐
                    │   CI/CD Pipeline            │
                    │   - Validate schema         │
                    │   - Get global ID           │
                    │   - Deploy to regions       │
                    └─────────┬─────────┬─────────┘
                              │         │
                    ┌─────────┴────┐    └─────────┐
                    ▼              ▼              ▼
            ┌──────────┐   ┌──────────┐   ┌──────────┐
            │  SR-US   │   │  SR-EU   │   │  SR-AP   │
            │ (IMPORT) │   │ (IMPORT) │   │ (IMPORT) │
            └────┬─────┘   └─────┬────┘   └─────┬────┘
                 │                │               │
        Schema Linking (for HA)  │     Schema Linking
                 └───────────────┼────────────────┘
                                 │
                          Bidirectional
                          for Failover
```

### Implementation Phases

**Phase 1: Foundation (Weeks 1-4)**
- Deploy Global Schema ID Coordinator
- Configure Schema Registries in IMPORT mode
- Set up CI/CD pipeline integration
- Pilot with 2 regions

**Phase 2: Schema Linking (Weeks 5-8)**
- Configure bidirectional Schema Linking
- Test failover scenarios
- Validate Schema ID consistency
- Document runbooks

**Phase 3: Workload Migration (Weeks 9-12)**
- Migrate first producer workload
- Validate end-to-end flow
- Test migration back procedure
- Train operations team

**Phase 4: Production Rollout (Weeks 13-24)**
- Migrate remaining workloads
- Monitor and optimize
- Continuous improvement

---

## 6. Conclusion

The Allianz Confluent Cloud migration requires a sophisticated approach to Schema Registry management that balances:

- **Scale:** Overcoming the 20k limit
- **Consistency:** Global unique Schema IDs
- **Availability:** Failover and recovery
- **Mobility:** Workload portability

**Recommended Solution:**
✅ **External Injection (3.3) + Schema Linking (3.2)**

This combination provides:
- Global scale through selective deployment
- HA through Schema Linking
- Workload mobility through global IDs
- Automated failover and recovery

**Next Steps:**
1. Review and approve this analysis
2. Design detailed coordinator architecture
3. Begin Phase 1 implementation
4. Pilot in non-production environment
5. Iterate based on learnings

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Review Status:** Draft - Awaiting Customer Input
