# Schema ID Creation Procedures

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Audience:** Platform Engineers, Schema Registry Administrators

---

## Table of Contents

1. [Overview](#overview)
2. [Schema ID Creation via REST API](#schema-id-creation-via-rest-api)
3. [Schema ID Creation via Schema Linking](#schema-id-creation-via-schema-linking)
4. [Comparison Matrix](#comparison-matrix)
5. [Best Practices](#best-practices)
6. [Troubleshooting](#troubleshooting)

---

## Overview

Confluent Schema Registry supports two primary methods for creating and managing Schema IDs:

1. **REST API-Based Creation**: Direct schema registration via HTTP endpoints
2. **Schema Linking**: Automated schema replication between Schema Registry clusters

Both methods must ensure **unique Schema IDs** within a context to maintain data consistency and enable proper serialization/deserialization across Kafka ecosystems.

---

## Schema ID Creation via REST API

### Architecture Overview

When schemas are registered via the REST API, the Schema ID creation process depends on the **mode** the Schema Registry is operating in:

- **READWRITE Mode**: Auto-generate IDs sequentially
- **IMPORT Mode**: Accept externally provided IDs
- **READONLY Mode**: No new schemas allowed
- **FORWARD Mode**: Forward requests to primary cluster

### Procedure 1: Auto-Generated Schema IDs (READWRITE Mode)

This is the **default behavior** for Schema Registry in standard operation.

#### Prerequisites

- Schema Registry running in READWRITE mode (default)
- Subject not in READONLY mode

#### Step-by-Step Process

**Step 1: Client Sends Registration Request**

```bash
curl -X POST http://localhost:8081/subjects/my-topic-value/versions \
  -H "Content-Type: application/json" \
  --data '{
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}",
    "schemaType": "AVRO"
  }'
```

**Step 2: Schema Registry Receives Request**

The request is handled by `SubjectVersionsResource.register()`:
- **File**: `core/src/main/java/io/confluent/kafka/schemaregistry/rest/resources/SubjectVersionsResource.java:433-510`
- **Request Model**: `RegisterSchemaRequest` (does not contain `id` field)

**Step 3: Leader Election Check**

The Schema Registry determines if it is the leader:
- **Leader**: Proceed with registration locally
- **Follower**: Forward request to leader

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaSchemaRegistry.java:616-626`

```java
if (isLeader()) {
  return register(subject, request, normalize);
} else {
  // forward registering request to the leader
  if (leaderIdentity != null) {
    return forwardRegisterRequestToLeader(subject, request, normalize, headerProperties);
  } else {
    throw new UnknownLeaderException("Register schema request failed since leader is unknown");
  }
}
```

**Step 4: Schema Duplicate Check**

Before generating a new ID, the registry checks if the schema already exists:
- Compute canonical form of the schema
- Look up schema by content (not ID)
- If found, return existing schema with its ID

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaSchemaRegistry.java:458-473`

**Step 5: ID Generation**

If the schema is new, generate a unique ID using `IncrementalIdGenerator`:

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/id/IncrementalIdGenerator.java:43-46`

```java
@Override
public int id(SchemaValue schema) throws IdGenerationException {
  String context = QualifiedSubject.contextFor(schemaRegistry.tenant(), schema.getSubject());
  return maxIds.compute(context, (k, v) -> v != null ? v + 1 : 1);
}
```

**ID Generation Algorithm:**
1. Extract context from subject (tenant + namespace)
2. Lookup current max ID for that context
3. Return `maxId + 1` (starts at 1 if none exists)
4. Store in concurrent map for thread-safety

**Step 6: ID Collision Check**

Verify the generated ID is not already in use (rare race condition):

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaSchemaRegistry.java:530-546`

```java
String qctx = QualifiedSubject.qualifiedContextFor(tenant(), subject);
int retries = 0;
while (retries++ < kafkaStoreMaxRetries) {
  int newId = idGenerator.id(schemaValue);
  // Verify id is not already in use
  if (lookupCache.schemaKeyById(newId, qctx) == null) {
    schema.setId(newId);
    schemaValue.setId(newId);
    if (retries > 1) {
      log.warn("Retrying to register the schema with ID {}", newId);
    }
    break;
  }
}
```

**Retry Logic:**
- Max retries: Configurable via `kafkastore.write.max.retries` (default: 5)
- If all retries fail: Throw `SchemaRegistryStoreException`

**Step 7: Persist to Kafka Store**

Write the schema with assigned ID to the backing Kafka topic:

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaSchemaRegistry.java:521-558`

```java
final SchemaKey schemaKey = new SchemaKey(subject, schema.getVersion());
final SchemaValue schemaValue = new SchemaValue(schema, ruleSetHandler);
metadataEncoder.encodeMetadata(schemaValue);

// ... ID assignment logic ...

kafkaStore.put(schemaKey, schemaValue);
logSchemaOp(schema, "REGISTER");
return schema;
```

**Step 8: Response to Client**

Return the registered schema with its ID:

```json
{
  "id": 1
}
```

#### Characteristics of Auto-Generated IDs

- ✅ **Sequential**: IDs increment by 1 within each context
- ✅ **Context-Isolated**: Different contexts have independent ID sequences
- ✅ **Collision-Safe**: Retry mechanism prevents race conditions
- ✅ **Deterministic**: Same schema always gets same ID (content-based lookup)
- ⚠️ **Cluster-Local**: Each isolated cluster generates independent IDs

---

### Procedure 2: Custom Schema IDs (IMPORT Mode)

IMPORT mode allows external systems to provide pre-assigned Schema IDs, enabling global coordination across multiple clusters.

#### Prerequisites

1. **Enable Mode Mutability** (in `schema-registry.properties`):
   ```properties
   mode.mutability=true
   ```

2. **Restart Schema Registry** after configuration change

3. **Set IMPORT Mode** via REST API

#### Step-by-Step Process

**Step 1: Configure IMPORT Mode**

Set the target subject or global context to IMPORT mode:

**Global (all subjects):**
```bash
curl -X PUT http://localhost:8081/mode \
  -H "Content-Type: application/json" \
  --data '{"mode": "IMPORT"}'
```

**Subject-specific:**
```bash
curl -X PUT http://localhost:8081/mode/my-topic-value \
  -H "Content-Type: application/json" \
  --data '{"mode": "IMPORT"}'
```

**Query Parameters:**
- `?force=true` - Force mode change even if schemas exist

**Step 2: Client Sends Registration with Custom ID**

```bash
curl -X POST http://localhost:8081/subjects/my-topic-value/versions \
  -H "Content-Type: application/json" \
  --data '{
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}",
    "schemaType": "AVRO",
    "id": 10000
  }'
```

**Key Difference**: The `id` field is **explicitly provided** by the client.

**Step 3: Mode Validation**

Schema Registry validates that custom IDs are allowed:

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/AbstractSchemaRegistry.java:338-350`

```java
if (schema.getId() >= 0) {
  if (!getModeInScope(subject).isImportOrForwardMode()) {
    throw new OperationNotPermittedException("Subject " + subject
            + " in context " + context + " is not in import mode");
  }
}
```

**Validation Rules:**
- ✅ ID must be non-negative (`>= 0`)
- ✅ Subject must be in IMPORT or FORWARD mode
- ❌ READWRITE mode rejects custom IDs

**Step 4: ID Conflict Check**

Verify the provided ID doesn't conflict with existing schemas:

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/AbstractSchemaRegistry.java:767-782`

```java
public void checkIfSchemaWithIdExist(int id, Schema schema) {
  String qctx = QualifiedSubject.qualifiedContextFor(tenant(), schema.getSubject());
  SchemaKey existingKey = this.lookupCache.schemaKeyById(id, qctx);
  if (existingKey != null) {
    SchemaRegistryValue existingValue = this.lookupCache.get(existingKey);
    if (existingValue instanceof SchemaValue existingSchemaValue) {
      Schema existingSchema = toSchemaEntity(existingSchemaValue);
      Schema schemaCopy = schema.copy();
      schemaCopy.setId(existingSchema.getId());
      schemaCopy.setSubject(existingSchema.getSubject());
      schemaCopy.setVersion(existingSchema.getVersion());
      if (!existingSchema.equals(schemaCopy)) {
        throw new OperationNotPermittedException(
          String.format("Overwrite new schema with id %s in context...", id));
      }
    }
  }
}
```

**Conflict Resolution:**
- **ID exists + same schema content**: ✅ Idempotent success
- **ID exists + different schema**: ❌ `OperationNotPermittedException`
- **ID available**: ✅ Proceed with registration

**Step 5: Skip Compatibility Checks**

In IMPORT mode, compatibility validation is **skipped**:

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaSchemaRegistry.java:493-500`

```java
boolean isCompatible = true;
List<String> compatibilityErrorLogs = new ArrayList<>();
if (!mode.isImportOrForwardMode()) {
  // sort undeleted in ascending
  Collections.reverse(undeletedVersions);
  compatibilityErrorLogs.addAll(isCompatibleWithPrevious(config,
      parsedSchema,
      undeletedVersions));
  isCompatible = compatibilityErrorLogs.isEmpty();
}
```

**Rationale**: External coordinator is responsible for compatibility

**Step 6: Direct ID Assignment**

The provided ID is used directly without generation:

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaSchemaRegistry.java:524-527`

```java
if (schemaId >= 0) {
  checkIfSchemaWithIdExist(schemaId, schema);
  schema.setId(schemaId);
  schemaValue.setId(schemaId);
}
```

**Step 7: Update ID Generator Max**

Even with custom IDs, the ID generator tracks the maximum:

**File**: `core/src/main/java/io/confluent/kafka/schemaregistry/id/IncrementalIdGenerator.java:65-71`

```java
@Override
public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
  String context = QualifiedSubject.contextFor(schemaRegistry.tenant(), schemaKey.getSubject());
  maxIds.compute(context, (k, v) -> {
    int id = v != null ? v : 1;
    return Math.max(schemaValue.getId(), id);
  });
}
```

**Purpose**: Ensures any future auto-generated IDs don't conflict

**Step 8: Persist and Respond**

Same as auto-generated flow:
- Write to Kafka store
- Return schema with ID to client

```json
{
  "id": 10000
}
```

#### Characteristics of Custom IDs

- ✅ **Externally Managed**: Coordinator controls ID allocation
- ✅ **Gap Tolerant**: IDs don't need to be sequential (e.g., 1, 5, 100)
- ✅ **Global Uniqueness**: Enabled by centralized coordinator
- ✅ **Idempotent**: Re-registering same schema+ID succeeds
- ⚠️ **No Compatibility Checks**: External system responsible
- ⚠️ **Requires IMPORT Mode**: Must configure mode before registration

---

## Schema ID Creation via Schema Linking

### Overview

**Schema Linking** is Confluent's native feature for replicating schemas between Schema Registry clusters with **ID preservation**. It automatically synchronizes schemas from a source cluster to a destination cluster.

**Key Feature**: Schema IDs are **preserved exactly** during replication.

### Architecture Components

#### 1. Schema Exporter (Source)

- Resides in the **source** Schema Registry
- Continuously monitors schema changes
- Exports schemas to destination

#### 2. Schema Linking Agent

- Replication engine
- Works in tandem with Cluster Linking
- Maintains sync between source and destination

#### 3. Destination Schema Registry

- Must be in **IMPORT mode**
- Receives schemas with original IDs preserved
- Creates schemas under destination context

### Procedure: Schema Linking Setup and Operation

#### Prerequisites

- Confluent Platform 7.0.0 or newer (recommended)
- Two Schema Registry clusters (source and destination)
- Network connectivity between clusters
- `mode.mutability=true` enabled on destination

#### Step 1: Configure Source Schema Registry

The source cluster operates in **READWRITE** or **READONLY** mode:

**For Active Replication (READWRITE):**
```bash
curl -X PUT http://source-sr:8081/mode \
  -H "Content-Type: application/json" \
  --data '{"mode": "READWRITE"}'
```

**For Migration (READONLY):**
```bash
curl -X PUT http://source-sr:8081/mode \
  -H "Content-Type: application/json" \
  --data '{"mode": "READONLY"}'
```

**Mode Selection:**
- **READWRITE**: Active-passive replication, source accepts new schemas
- **READONLY**: Migration scenario, source is read-only

#### Step 2: Prepare Destination Schema Registry

**Requirement**: Destination must be **empty** or use context isolation

**Set IMPORT Mode (Global):**
```bash
curl -X PUT http://destination-sr:8081/mode \
  -H "Content-Type: application/json" \
  --data '{"mode": "IMPORT"}' \
  --data-urlencode 'force=true'
```

**Set IMPORT Mode (Specific Context):**
```bash
curl -X PUT http://destination-sr:8081/mode/:lsrc-source-cluster \
  -H "Content-Type: application/json" \
  --data '{"mode": "IMPORT"}'
```

**Context Naming Convention:**
- Default: `.lsrc-<source-logical-name>`
- Example: `.lsrc-us-west-primary`

#### Step 3: Create Schema Exporter

Configure the exporter on the **source** Schema Registry:

```bash
curl -X POST http://source-sr:8081/exporters \
  -H "Content-Type: application/json" \
  --data '{
    "name": "exporter-to-east",
    "contextType": "CUSTOM",
    "context": ".lsrc-us-west",
    "subjects": ["*"],
    "subjectRenameFormat": "${subject}",
    "config": {
      "schema.registry.url": "http://destination-sr:8081",
      "basic.auth.credentials.source": "USER_INFO",
      "basic.auth.user.info": "user:password"
    }
  }'
```

**Configuration Parameters:**
- **name**: Unique exporter identifier
- **contextType**: `CUSTOM` for specific context, `AUTO` for default
- **context**: Source context to export (or destination context to import into)
- **subjects**: Array of subject patterns (`["*"]` for all, or specific subjects)
- **subjectRenameFormat**: How to rename subjects (use `${subject}` to preserve names)
- **config.schema.registry.url**: Destination Schema Registry URL
- **Authentication**: Basic auth credentials for destination

#### Step 4: Schema Replication Process

Once configured, Schema Linking operates automatically:

**Phase 1: Initial Sync**

1. Exporter scans all schemas in source context
2. For each schema:
   - Extract schema content, ID, version, subject
   - Send to destination via REST API with **custom ID**

**Destination Registration Request (Internal):**
```json
{
  "schema": "...",
  "schemaType": "AVRO",
  "id": 42,
  "version": 1
}
```

**Phase 2: Continuous Replication**

3. Exporter monitors source Schema Registry for changes
4. New schemas automatically replicated with original IDs
5. Schema updates synchronized in real-time

**Phase 3: ID Preservation**

The destination Schema Registry receives schemas in IMPORT mode:
- Custom ID from source is used directly
- No new ID generation occurs
- Collision checks ensure integrity

**File**: Same validation as IMPORT mode procedure
- `AbstractSchemaRegistry.java:767-782` - ID conflict check
- `KafkaSchemaRegistry.java:524-527` - Direct ID assignment

#### Step 5: Verify Replication

**Check Exporter Status:**
```bash
curl http://source-sr:8081/exporters/exporter-to-east/status
```

**Response:**
```json
{
  "name": "exporter-to-east",
  "state": "RUNNING",
  "offset": 150,
  "ts": 1699999999000
}
```

**Verify Schema Exists on Destination:**
```bash
# Get schema by ID (should match source)
curl http://destination-sr:8081/schemas/ids/42

# Compare with source
curl http://source-sr:8081/schemas/ids/42
```

**Expected Result**: Identical schema content and ID

#### Step 6: Monitor Replication Lag

```bash
# Check exporter metrics
curl http://source-sr:8081/exporters/exporter-to-east/metrics

# Typical metrics:
# - schemas_exported_total
# - schemas_failed_total
# - replication_lag_seconds
```

### Context Isolation for ID Collision Avoidance

If the destination cluster already has schemas, use **contexts** to prevent ID collisions:

#### Scenario: Destination Has Existing Schemas

**Problem**: Source schema ID 42 conflicts with existing destination schema ID 42

**Solution**: Import into a separate context

**Step 1: Create Exporter with Context Mapping:**
```bash
curl -X POST http://source-sr:8081/exporters \
  -H "Content-Type: application/json" \
  --data '{
    "name": "exporter-with-context",
    "contextType": "CUSTOM",
    "context": ".lsrc-source-region",
    "subjects": ["*"],
    "config": {
      "schema.registry.url": "http://destination-sr:8081"
    }
  }'
```

**Step 2: Access Schemas via Context:**
```bash
# Destination schema with context prefix
curl http://destination-sr:8081/subjects/:.lsrc-source-region:my-topic-value/versions
```

**Context Benefits:**
- ✅ **Independent ID Spaces**: Context A ID 42 ≠ Context B ID 42
- ✅ **Multi-Source**: Import from multiple source clusters
- ✅ **Isolation**: No conflicts between contexts
- ✅ **Namespacing**: Logical separation of schemas

### Schema Linking Characteristics

- ✅ **ID Preservation**: Source IDs replicated exactly
- ✅ **Automatic**: Continuous, real-time synchronization
- ✅ **Context Support**: Isolate schemas from different sources
- ✅ **Conflict-Free**: Validation prevents overwrites
- ✅ **Official Feature**: Supported by Confluent Platform
- ⚠️ **IMPORT Mode Required**: Destination must be in IMPORT mode
- ⚠️ **All or Nothing**: Cannot selectively filter schemas (without context)
- ⚠️ **License Required**: Enterprise feature for Confluent Platform

---

## Comparison Matrix

| Feature | REST API (READWRITE) | REST API (IMPORT) | Schema Linking |
|---------|---------------------|-------------------|----------------|
| **ID Generation** | Auto-incremental | Externally provided | Preserved from source |
| **Mode Required** | READWRITE (default) | IMPORT | IMPORT (destination) |
| **ID Control** | ❌ Registry-managed | ✅ Client-managed | ✅ Source-managed |
| **Compatibility Checks** | ✅ Enforced | ❌ Skipped | ❌ Skipped (destination) |
| **Sequential IDs** | ✅ Guaranteed | ❌ Gaps allowed | ✅ If source sequential |
| **Cross-Cluster Coordination** | ❌ Independent per cluster | ✅ Via coordinator | ✅ Via replication |
| **ID Collisions** | ✅ Prevented by generator | ⚠️ External responsibility | ✅ Prevented by validation |
| **Use Case** | Single-cluster standard ops | Multi-cluster with coordinator | Multi-cluster replication |
| **Automation** | Manual API calls | Manual/CI-CD | Automatic continuous sync |
| **Context Isolation** | ✅ Supported | ✅ Supported | ✅ Strongly recommended |
| **Version Control** | ✅ Sequential | ✅ Can specify | ✅ Preserved |
| **Schema Filtering** | ✅ Full control | ✅ Full control | ⚠️ Limited (subject patterns) |
| **License** | ✅ Open source | ✅ Open source | ⚠️ Enterprise feature |

---

## Best Practices

### For REST API (Auto-Generated IDs)

1. **Use for Single-Cluster Deployments**
   - Default mode is sufficient
   - No coordination overhead
   - Simple operational model

2. **Monitor ID Exhaustion**
   - Track current max ID per context
   - Alert before reaching INT_MAX (2,147,483,647)

3. **Enable Context Isolation for Multi-Tenancy**
   - Separate ID spaces per tenant
   - Use qualified subjects: `:tenant-name:subject`

### For REST API (Custom IDs with IMPORT Mode)

1. **Implement Global ID Coordinator**
   - Centralized ID allocation service
   - Ensure uniqueness across all regions
   - Provide idempotent ID reservation

2. **Set IMPORT Mode Before First Schema**
   - Transition is easier with empty registry
   - Use `?force=true` if schemas exist

3. **Validate IDs Before Registration**
   - Check ID availability in target cluster
   - Handle collisions gracefully
   - Implement retry logic

4. **Document ID Ranges**
   - Maintain registry of ID allocations
   - Track gaps and reserved ranges
   - Audit trail for compliance

5. **Test Idempotency**
   - Verify re-registration succeeds
   - Ensure CI/CD pipeline is idempotent
   - Handle network failures gracefully

### For Schema Linking

1. **Use for Active-Passive Replication**
   - Primary: READWRITE mode
   - Secondary: IMPORT mode
   - Automatic failover preparation

2. **Leverage Contexts for Multi-Source**
   - One context per source cluster
   - Prevents ID collisions
   - Clear organizational structure

3. **Monitor Replication Lag**
   - Alert on lag > threshold (e.g., 60 seconds)
   - Track failed export metrics
   - Automated health checks

4. **Plan for Disaster Recovery**
   - Document failover procedures
   - Test mode transitions (IMPORT → READWRITE)
   - Backup exporter configurations

5. **Start with Readonly Source for Migration**
   - Prevents schema changes during migration
   - Clean cutover to destination
   - Reduce risk of inconsistencies

---

## Troubleshooting

### REST API Issues

#### Problem: "Subject is not in import mode"

**Error:**
```json
{
  "error_code": 42205,
  "message": "Subject my-topic-value in context .default is not in import mode"
}
```

**Solution:**
```bash
# Set IMPORT mode for the subject or globally
curl -X PUT http://localhost:8081/mode/my-topic-value \
  -H "Content-Type: application/json" \
  --data '{"mode": "IMPORT"}'
```

#### Problem: "Overwrite new schema with id X"

**Error:**
```json
{
  "error_code": 42205,
  "message": "Overwrite new schema with id 100 in context .default"
}
```

**Cause:** ID 100 already exists with different schema content

**Solutions:**
1. Use a different ID
2. Verify the existing schema:
   ```bash
   curl http://localhost:8081/schemas/ids/100
   ```
3. Check if this is the correct ID for the schema

#### Problem: "ID generation collision after max retries"

**Error:**
```
SchemaRegistryStoreException: Error while registering the schema due to generating an ID that is already in use.
```

**Cause:** Extremely high concurrent registration rate or ID space exhaustion

**Solutions:**
1. Increase `kafkastore.write.max.retries` in config
2. Implement client-side backoff and retry
3. Consider using IMPORT mode with coordinated IDs

### Schema Linking Issues

#### Problem: Exporter stuck in "PAUSED" state

**Check:**
```bash
curl http://source-sr:8081/exporters/my-exporter/status
```

**Common Causes:**
1. Destination Schema Registry unreachable
2. Authentication failure
3. Destination not in IMPORT mode

**Solutions:**
1. Verify network connectivity
2. Test destination credentials
3. Check destination mode:
   ```bash
   curl http://destination-sr:8081/mode
   ```

#### Problem: Schema ID mismatch between source and destination

**Verification:**
```bash
# Source
SOURCE_SCHEMA=$(curl http://source-sr:8081/schemas/ids/42)

# Destination
DEST_SCHEMA=$(curl http://destination-sr:8081/schemas/ids/42)

# Compare
diff <(echo "$SOURCE_SCHEMA" | jq -S .) <(echo "$DEST_SCHEMA" | jq -S .)
```

**Cause:** Destination was not in IMPORT mode during replication

**Solution:**
1. Delete incorrect schema from destination (if possible)
2. Set IMPORT mode on destination
3. Restart exporter to re-sync

#### Problem: Cannot set IMPORT mode - "found existing subjects"

**Error:**
```json
{
  "error_code": 42205,
  "message": "Cannot import since found existing subjects"
}
```

**Solutions:**

**Option 1: Use Force Flag**
```bash
curl -X PUT "http://localhost:8081/mode?force=true" \
  -H "Content-Type: application/json" \
  --data '{"mode": "IMPORT"}'
```

**Option 2: Use Context Isolation**
```bash
# Import into separate context
curl -X PUT http://localhost:8081/mode/:lsrc-source \
  -H "Content-Type: application/json" \
  --data '{"mode": "IMPORT"}'
```

**Option 3: Migration Scenario**
- Delete existing subjects (if acceptable)
- Transition to IMPORT mode
- Re-import all schemas

---

## Summary

### Key Takeaways

1. **REST API (READWRITE Mode)**
   - Best for: Single-cluster standard operations
   - ID generation: Automatic, sequential, collision-safe
   - Complexity: Low

2. **REST API (IMPORT Mode)**
   - Best for: Multi-cluster with global coordination
   - ID generation: External coordinator provides IDs
   - Complexity: High (requires coordinator service)

3. **Schema Linking**
   - Best for: Multi-cluster replication and DR
   - ID generation: Preserved from source cluster
   - Complexity: Medium (enterprise feature)

### Decision Matrix

**Choose REST API (READWRITE) when:**
- ✅ Single Schema Registry cluster
- ✅ No cross-cluster coordination needed
- ✅ Sequential IDs are acceptable

**Choose REST API (IMPORT) when:**
- ✅ Multiple independent clusters
- ✅ Need global unique IDs
- ✅ Selective schema deployment per region
- ✅ CI/CD-driven schema management

**Choose Schema Linking when:**
- ✅ Active-passive replication required
- ✅ Disaster recovery preparation
- ✅ Automatic continuous synchronization
- ✅ All schemas should be replicated

---

## References

### Code References

- **ID Generator**: `core/src/main/java/io/confluent/kafka/schemaregistry/id/IncrementalIdGenerator.java`
- **Registration Logic**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaSchemaRegistry.java:412-561`
- **Mode Validation**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/AbstractSchemaRegistry.java:338-350`
- **ID Conflict Check**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/AbstractSchemaRegistry.java:767-782`
- **REST Resource**: `core/src/main/java/io/confluent/kafka/schemaregistry/rest/resources/SubjectVersionsResource.java`

### Documentation

- Confluent Schema Registry API Reference
- Schema Linking for Confluent Platform
- Schema Registry Multi-Datacenter Deployments
- Schema Registry Migration Guide

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Maintained By:** Platform Engineering Team
