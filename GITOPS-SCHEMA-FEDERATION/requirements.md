# GitOps Schema Federation Manager - Requirements Specification

**Version:** 1.0
**Date:** 2025-11-13
**Status:** Draft for Implementation

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Business Requirements](#business-requirements)
3. [Functional Requirements](#functional-requirements)
4. [Non-Functional Requirements](#non-functional-requirements)
5. [System Requirements](#system-requirements)
6. [User Interface Requirements](#user-interface-requirements)
7. [Integration Requirements](#integration-requirements)
8. [Security Requirements](#security-requirements)
9. [Acceptance Criteria](#acceptance-criteria)

---

## Executive Summary

### Purpose

The **GitOps Schema Federation Manager** is a comprehensive solution for managing Kafka schema deployment across multiple Schema Registry instances (local and cloud) with globally coordinated Schema IDs. It provides:

- Centralized schema versioning in Git
- Global Schema ID coordination
- Multi-region schema deployment automation
- Schema compatibility validation
- Web-based schema management UI
- Health monitoring and drift detection

### Problem Statement

Operating Kafka clusters across multiple regions with independent Schema Registries leads to:
- Schema ID conflicts when data flows between regions
- Lack of central visibility into schema versions
- Manual, error-prone schema deployment processes
- No automated validation of schema compatibility
- Difficulty tracking which schemas are deployed where

### Solution Overview

A GitOps-based platform that treats schemas as code, providing:
- Git as the single source of truth for all schemas
- Global Schema ID allocation service
- Automated CI/CD pipeline for schema deployment
- Web UI for schema management and monitoring
- Multi-region deployment with selective schema distribution

---

## Business Requirements

### BR-1: GitOps Workflow
**Description:** All schema changes must follow GitOps principles
**Priority:** CRITICAL
**Rationale:** Version control, auditability, and reproducible deployments

**Acceptance Criteria:**
- All schemas stored in Git repository
- Changes require pull request approval
- Deployment triggered by Git commits/tags
- Full audit trail of schema changes

### BR-2: Global Schema ID Coordination
**Description:** Schema IDs must be globally unique across all regions
**Priority:** CRITICAL
**Rationale:** Prevent ID conflicts when data flows between regions

**Acceptance Criteria:**
- Single ID allocation service
- IDs never reused or conflicting
- ID assignment is idempotent
- ID allocation tracked in Git

### BR-3: Multi-Region Deployment
**Description:** Deploy schemas selectively to relevant regions
**Priority:** HIGH
**Rationale:** Optimize registry usage and avoid 20,000 schema limit

**Acceptance Criteria:**
- Schema-to-region mapping configuration
- Automated deployment to multiple registries
- Support for local and cloud registries
- Deployment status tracking per region

### BR-4: Schema Compatibility Validation
**Description:** Validate schema evolution against compatibility rules
**Priority:** HIGH
**Ratability:** Prevent breaking changes in production

**Acceptance Criteria:**
- Pre-deployment compatibility checks
- Support all compatibility modes (BACKWARD, FORWARD, FULL, NONE)
- Validation before ID allocation
- Clear error messages on incompatibility

### BR-5: Health Monitoring
**Description:** Monitor schema synchronization across regions
**Priority:** MEDIUM
**Rationale:** Detect drift and deployment failures

**Acceptance Criteria:**
- Compare Git vs deployed schemas
- Detect missing or extra schemas
- Alert on compatibility issues
- Dashboard showing deployment health

---

## Functional Requirements

### F-1: Schema Storage and Organization

#### F-1.1: Folder Structure
**Description:** Organize schemas in Git repository by context, subject, and version

**Requirements:**
- Directory structure: `SCHEMASTORE/{context}/{subject}/{version}/`
- Each version folder contains:
  - `schema.avsc` (or `.proto`, `.json`)
  - `metadata.json` (ID, version, compatibility mode, regions)
- Support for multiple schema types (Avro, Protobuf, JSON Schema)

**Example Structure:**
```
SCHEMASTORE/
├── default/
│   ├── user-events-value/
│   │   ├── v1/
│   │   │   ├── schema.avsc
│   │   │   └── metadata.json
│   │   ├── v2/
│   │   │   ├── schema.avsc
│   │   │   └── metadata.json
│   └── order-events-key/
│       └── v1/
│           ├── schema.avsc
│           └── metadata.json
├── analytics/
│   └── clickstream-value/
│       └── v1/
│           ├── schema.avsc
│           └── metadata.json
```

#### F-1.2: Metadata Schema
**Description:** Define metadata.json structure

```json
{
  "schemaId": 10001,
  "subject": "user-events-value",
  "version": 1,
  "context": "default",
  "schemaType": "AVRO",
  "compatibilityMode": "BACKWARD",
  "deploymentRegions": ["us-west-local", "eu-central-local", "confluent-cloud"],
  "createdAt": "2025-11-13T10:00:00Z",
  "createdBy": "john.doe@company.com",
  "description": "User event payload schema",
  "tags": ["pii", "analytics"]
}
```

### F-2: Global Schema ID Management

#### F-2.1: ID Allocation Service
**Description:** Central service for allocating globally unique Schema IDs

**Requirements:**
- REST API for ID reservation
- Idempotent ID allocation (same schema content → same ID)
- Thread-safe concurrent allocation
- Persistent ID registry (database or Git-backed)
- Support for batch ID allocation

**API Endpoints:**
```
POST /api/v1/schema-ids/allocate
GET  /api/v1/schema-ids/{id}
GET  /api/v1/schema-ids?subject={subject}&version={version}
DELETE /api/v1/schema-ids/{id}  # For cleanup/testing only
```

#### F-2.2: ID Allocation Strategy
**Description:** Define ID allocation algorithm

**Requirements:**
- Sequential allocation starting from configurable base (e.g., 10000)
- Schema content hashing for duplicate detection
- Context-aware allocation (optional: different ranges per context)
- Gap tolerance (IDs don't need to be consecutive)

### F-3: Schema Registration and Validation

#### F-3.1: Local Schema Addition
**Description:** Add new schema via CLI or Web UI

**Requirements:**
- Upload schema file (Avro, Protobuf, JSON)
- Specify subject, context, compatibility mode
- Auto-detect schema type
- Validate schema syntax
- Check compatibility against previous versions
- Allocate globally unique ID
- Generate metadata.json
- Create version folder structure
- Commit to Git (or create branch for PR)

**CLI Usage:**
```bash
./gitops-schema add \
  --subject user-events-value \
  --context default \
  --schema-file user.avsc \
  --compatibility BACKWARD \
  --regions us-west-local,confluent-cloud \
  --description "User event payload"
```

#### F-3.2: Compatibility Validation
**Description:** Validate schema evolution before deployment

**Requirements:**
- Load previous version from Git
- Parse both schemas
- Run compatibility check based on mode:
  - BACKWARD: New schema can read old data
  - FORWARD: Old schema can read new data
  - FULL: Both directions compatible
  - BACKWARD_TRANSITIVE: Compatible with all previous versions
  - FORWARD_TRANSITIVE: All previous can read new
  - FULL_TRANSITIVE: Full compatibility with all versions
  - NONE: No validation
- Detailed error messages with field-level diff
- Pre-commit validation hook

#### F-3.3: Schema Validation Rules
**Description:** Syntax and semantic validation

**Requirements:**
- Valid JSON for Avro schemas
- Valid Protobuf syntax for .proto files
- Valid JSON Schema for JSON type
- No reserved keywords misuse
- Valid field types
- Proper namespaces and imports

### F-4: Deployment Automation

#### F-4.1: Region Mapping Configuration
**Description:** Define which schemas deploy to which regions

**Configuration File:** `deployment-config.yaml`
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
    subjects: ["*"]
    regions: ["us-west-local", "eu-central-local", "confluent-cloud"]

  - context: analytics
    subjects: ["clickstream-*"]
    regions: ["us-west-local"]

  - context: default
    subjects: ["gdpr-*"]
    regions: ["eu-central-local", "confluent-cloud"]
```

#### F-4.2: Deployment Script
**Description:** Automated deployment to all configured regions

**Requirements:**
- Read deployment-config.yaml
- For each schema version:
  - Read schema file and metadata
  - For each target region:
    - Check if SR is in IMPORT mode
    - Register schema with custom ID via REST API
    - Verify successful registration
    - Log deployment status
- Support dry-run mode
- Idempotent (can re-run safely)
- Parallel deployment to multiple regions
- Rollback on failure (optional)

**Deployment Command:**
```bash
./deploy-schemas.sh --config deployment-config.yaml --dry-run
./deploy-schemas.sh --config deployment-config.yaml --subjects user-events-value
./deploy-schemas.sh --config deployment-config.yaml --regions us-west-local
```

#### F-4.3: CI/CD Integration
**Description:** GitHub Actions / GitLab CI pipeline

**Pipeline Stages:**
1. **Validate**: Syntax and compatibility checks
2. **Test**: Deploy to test SR
3. **Approve**: Manual approval gate
4. **Deploy**: Deploy to production SRs
5. **Verify**: Health check post-deployment

**Triggers:**
- Pull request: Run validation only
- Merge to main: Deploy to all regions
- Tag creation: Deploy specific version

### F-5: Web UI

#### F-5.1: Schema Management UI

**Pages:**

**1. Dashboard**
- Total schemas count
- Schemas by context/subject
- Recent schema changes
- Deployment status overview
- Health alerts

**2. Schema Browser**
- Tree view: Context → Subject → Versions
- Search by subject name
- Filter by context, schema type, tags
- View schema content (syntax highlighted)
- View metadata
- Download schema file

**3. Add New Schema**
- Form fields:
  - Subject name (autocomplete from existing)
  - Context (dropdown)
  - Schema type (Avro/Protobuf/JSON)
  - Upload schema file or paste content
  - Compatibility mode (dropdown)
  - Target regions (multi-select)
  - Description
  - Tags
- Real-time validation feedback
- Compatibility check results
- Preview generated metadata
- Submit → Create Git branch + PR

**4. Schema Registry Connections**
- List all configured SRs
- Add new SR connection:
  - Name
  - URL
  - Auth type (none/basic/SSL)
  - Credentials
  - Test connection button
- Edit/Delete connections
- Connectivity status indicator

**5. Health Dashboard**
- Per-region deployment status
- Git vs deployed schema diff
- Missing schemas alert
- Extra schemas (not in Git) alert
- Compatibility violations
- Sync lag metrics

#### F-5.2: SR Connection Management

**Requirements:**
- Add/Edit/Delete SR connections
- Test connectivity before saving
- Validate SR mode (warn if not IMPORT)
- Ping endpoint: `GET /schemas/types`
- Authentication test
- Store credentials securely (encrypted or env vars)

**Test Connection Flow:**
```
User clicks "Test Connection"
  ↓
UI sends: POST /api/v1/registries/test
  {
    "url": "http://localhost:8081",
    "authType": "basic",
    "username": "user",
    "password": "pass"
  }
  ↓
Backend attempts:
  1. GET {url}/schemas/types
  2. GET {url}/mode
  3. Return status + mode info
  ↓
UI displays:
  ✓ Connection successful
  ℹ Mode: IMPORT
  or
  ✗ Connection failed: timeout
```

#### F-5.3: Schema Discovery and Comparison

**Requirements:**
- Load all schemas from all connected SRs
- Compare with SCHEMASTORE in Git
- Generate comparison report:
  - ✓ In sync: Same ID, same content
  - ⚠ Drift: Different content for same ID
  - ⚠ Missing: In Git but not deployed
  - ⚠ Extra: Deployed but not in Git
  - ✗ Conflict: Different schema for same ID

**Comparison UI:**
```
┌─────────────────────────────────────────────────────────┐
│ Schema: user-events-value v2                            │
├─────────────────────────────────────────────────────────┤
│ Status: ⚠ DRIFT DETECTED                                │
│                                                          │
│ Git (Source of Truth):                                  │
│   ID: 10002                                             │
│   Content: {type: record, fields: [id, name, email]}    │
│                                                          │
│ us-west-local:                                          │
│   Status: ✓ In Sync                                     │
│   ID: 10002                                             │
│                                                          │
│ eu-central-local:                                       │
│   Status: ✗ CONFLICT                                    │
│   ID: 10002                                             │
│   Content: {type: record, fields: [id, name]}           │
│   Action: [Re-deploy from Git] [Ignore]                 │
│                                                          │
│ confluent-cloud:                                        │
│   Status: ⚠ MISSING                                     │
│   Action: [Deploy Now]                                  │
└─────────────────────────────────────────────────────────┘
```

### F-6: Validation and Testing

#### F-6.1: Pre-Commit Validation
**Description:** Git hooks to validate schemas before commit

**Requirements:**
- Validate schema syntax
- Check metadata.json structure
- Verify ID uniqueness
- Run compatibility checks
- Prevent commits if validation fails

**Hook:** `.git/hooks/pre-commit`

#### F-6.2: Integration Tests
**Description:** Automated tests for deployment pipeline

**Test Scenarios:**
- Add new schema → verify ID allocated
- Add incompatible schema → expect rejection
- Deploy to test SR → verify registration
- Deploy duplicate → verify idempotency
- SR unreachable → verify error handling
- Invalid credentials → verify error handling

---

## Non-Functional Requirements

### NFR-1: Performance

**NFR-1.1: ID Allocation**
- Allocate ID in < 100ms (p95)
- Support 100 concurrent allocations
- No ID collisions

**NFR-1.2: Deployment**
- Deploy 1 schema to 1 region in < 5 seconds
- Deploy 100 schemas to 3 regions in < 5 minutes
- Parallel deployment to multiple regions

**NFR-1.3: Web UI**
- Page load < 2 seconds
- Schema list rendering: 1000 schemas in < 1 second
- Real-time validation feedback < 500ms

### NFR-2: Reliability

**NFR-2.1: ID Allocation**
- 99.9% availability
- No duplicate IDs under any circumstances
- Persistent storage (survive restarts)

**NFR-2.2: Deployment**
- Idempotent deployments
- Retry on transient failures (3 retries with backoff)
- Atomic per-schema (all regions or none)
- Deployment logs for audit

**NFR-2.3: Data Integrity**
- Git as source of truth (never modify without commit)
- Validate schema content before deployment
- Verify post-deployment (read back from SR)

### NFR-3: Scalability

**NFR-3.1: Schema Storage**
- Support 10,000+ schemas
- Support 100+ versions per subject
- Support 10+ contexts
- Git repository < 1GB

**NFR-3.2: Deployment**
- Deploy to 10+ regions simultaneously
- Handle 50+ concurrent deployments
- Support 1000+ subjects

**NFR-3.3: Web UI**
- Handle 10,000+ schemas in browser
- Paginated lists (100 items per page)
- Lazy loading for large schema content

### NFR-4: Maintainability

**NFR-4.1: Code Quality**
- Unit test coverage > 80%
- Integration test coverage > 60%
- Linting and code formatting
- API documentation (OpenAPI/Swagger)

**NFR-4.2: Configuration**
- Externalized configuration (env vars, config files)
- No hardcoded URLs or credentials
- Docker-based deployment
- Health check endpoints

**NFR-4.3: Logging**
- Structured logging (JSON format)
- Log levels: DEBUG, INFO, WARN, ERROR
- Request/response logging
- Deployment audit logs

---

## System Requirements

### SR-1: Development Environment

**Required Software:**
- Docker & Docker Compose (latest)
- Git (2.30+)
- Node.js (18+) for Web UI
- Python (3.10+) for backend service
- curl, jq for testing

**Recommended IDE:**
- VS Code with extensions:
  - Avro syntax highlighting
  - Protobuf support
  - YAML/JSON schema validation

### SR-2: Runtime Environment

**Local Development:**
- 2x Kafka Clusters (via Docker Compose)
- 2x Schema Registry (via Docker Compose)
- 2x Control Center (via Docker Compose)
- Confluent Cloud account (for testing cloud deployment)

**Production:**
- Kubernetes cluster (optional)
- CI/CD platform (GitHub Actions / GitLab CI)
- PostgreSQL or SQLite for ID allocation service
- Nginx or Traefik for Web UI

### SR-3: Network Requirements

**Connectivity:**
- All Schema Registries reachable from deployment pod
- Outbound HTTPS to Confluent Cloud
- Inbound HTTP for Web UI (port 3000)
- Inbound HTTP for API service (port 8080)

**Firewall Rules:**
- Allow port 8081, 8082 (local SRs)
- Allow port 443 to *.confluent.cloud
- Allow port 3000, 8080 for internal services

---

## User Interface Requirements

### UIR-1: Responsiveness
- Support desktop browsers (Chrome, Firefox, Safari)
- Responsive design (min width: 1024px)
- Mobile-friendly (optional, Phase 2)

### UIR-2: Accessibility
- WCAG 2.1 Level A compliance
- Keyboard navigation
- Screen reader support
- High contrast mode

### UIR-3: User Experience
- Intuitive navigation
- Clear error messages
- Loading indicators for async operations
- Confirmation dialogs for destructive actions
- Undo capability (via Git revert)

### UIR-4: Framework
- React (18+) or Vue.js (3+)
- TypeScript for type safety
- Tailwind CSS or Material-UI for styling
- Monaco Editor for schema editing (VS Code editor component)

---

## Integration Requirements

### IR-1: Git Integration
- Read/Write to Git repository
- Create branches for new schemas
- Create pull requests (GitHub/GitLab API)
- Commit metadata and schema files
- Support SSH and HTTPS authentication

### IR-2: Schema Registry Integration
- REST API client for all SR operations
- Support basic auth, SSL, and OAuth
- Handle different SR versions (5.x, 6.x, 7.x+)
- Retry logic with exponential backoff
- Connection pooling

### IR-3: CI/CD Integration
- GitHub Actions workflow
- GitLab CI pipeline
- Environment-specific deployments (dev/staging/prod)
- Secrets management (GitHub Secrets, Vault)

### IR-4: Monitoring Integration (Optional)
- Prometheus metrics export
- Grafana dashboard
- Slack/Email notifications
- PagerDuty integration for critical alerts

---

## Security Requirements

### SEC-1: Authentication and Authorization

**SEC-1.1: Web UI Authentication**
- User login (OAuth 2.0 / OIDC)
- Session management
- Role-based access control (RBAC):
  - Viewer: Read-only access
  - Editor: Add/edit schemas
  - Admin: Manage SR connections, deploy

**SEC-1.2: API Authentication**
- API key-based authentication
- JWT tokens
- Rate limiting (100 requests/min per user)

### SEC-2: Data Security

**SEC-2.1: Credentials Storage**
- Never store plaintext credentials in Git
- Use environment variables
- Encrypt SR credentials at rest
- Use Kubernetes secrets or Vault

**SEC-2.2: Audit Logging**
- Log all schema additions/changes
- Log all deployments with user info
- Log SR connection changes
- Tamper-proof audit trail (append-only)

### SEC-3: Network Security

**SEC-3.1: Transport Encryption**
- HTTPS for all SR connections
- TLS 1.2+ only
- Certificate validation

**SEC-3.2: Input Validation**
- Sanitize all user inputs
- Validate schema content (prevent injection)
- Validate URLs (prevent SSRF)
- File upload restrictions (max 1MB, allowed extensions)

---

## Acceptance Criteria

### AC-1: Core Functionality

**AC-1.1: Schema Addition**
- ✅ User can add new schema via Web UI
- ✅ Schema gets globally unique ID
- ✅ Schema stored in correct folder structure
- ✅ Metadata.json created with all fields
- ✅ Compatibility validation runs successfully
- ✅ Changes committed to Git (or PR created)

**AC-1.2: Schema Deployment**
- ✅ Deployment script reads all schemas from SCHEMASTORE
- ✅ Schemas deployed to correct regions based on config
- ✅ Custom IDs used during registration
- ✅ Deployment idempotent (can re-run)
- ✅ Deployment logs created
- ✅ Post-deployment verification succeeds

**AC-1.3: Health Monitoring**
- ✅ UI shows all connected SRs
- ✅ UI loads schemas from all SRs
- ✅ UI compares Git vs deployed schemas
- ✅ UI displays drift/missing/conflict alerts
- ✅ User can trigger re-deployment from UI

### AC-2: Quality Assurance

**AC-2.1: Testing**
- ✅ Unit tests pass (coverage > 80%)
- ✅ Integration tests pass (test SR deployment)
- ✅ End-to-end test: Add schema → Deploy → Verify

**AC-2.2: Documentation**
- ✅ README with setup instructions
- ✅ API documentation (Swagger/OpenAPI)
- ✅ User guide for Web UI
- ✅ Deployment guide for operators

**AC-2.3: Code Quality**
- ✅ Linting passes (no errors)
- ✅ Code formatted consistently
- ✅ No security vulnerabilities (Snyk/Dependabot)
- ✅ Dependencies up to date

### AC-3: User Acceptance

**AC-3.1: Usability**
- ✅ Non-technical users can add schemas via UI
- ✅ Error messages are clear and actionable
- ✅ UI is responsive and fast
- ✅ No data loss (Git commits always succeed)

**AC-3.2: Reliability**
- ✅ No duplicate Schema IDs observed
- ✅ Deployment succeeds 99% of the time
- ✅ Service recovers from transient failures
- ✅ Data integrity maintained (Git = SR)

---

## Out of Scope (Phase 1)

The following features are explicitly **not included** in the initial release:

1. **Schema Migration**: Migrating existing schemas from SRs to Git
2. **Schema Deletion**: Deleting schemas from SRs (soft delete only)
3. **Advanced RBAC**: Fine-grained permissions per subject/context
4. **Multi-Tenancy**: Isolated environments for different teams
5. **Schema Lineage**: Visualize dependencies between schemas
6. **Performance Testing**: Load testing with 10,000+ schemas
7. **Schema Evolution Suggestions**: AI-powered compatibility fixes
8. **Mobile App**: Native mobile application
9. **Real-Time Sync**: Bi-directional sync between Git and SR
10. **Schema Marketplace**: Public registry of reusable schemas

These may be considered for future phases based on user feedback.

---

## Glossary

- **Context**: Namespace in Schema Registry for isolating schemas
- **Subject**: Unique name for a schema (e.g., "user-events-value")
- **Version**: Incremental version number for schema evolution
- **Schema ID**: Globally unique integer identifier for a schema
- **IMPORT Mode**: Schema Registry mode allowing custom IDs
- **Compatibility Mode**: Rules for schema evolution (BACKWARD, FORWARD, FULL, etc.)
- **GitOps**: Infrastructure/configuration managed via Git with automated deployment
- **Drift**: Difference between Git (source of truth) and deployed state

---

## Appendix A: Schema Type Support

### Avro
- ✅ Primitive types: string, int, long, float, double, boolean, null, bytes
- ✅ Complex types: record, array, map, union, enum, fixed
- ✅ Logical types: date, time, timestamp, decimal, uuid
- ✅ References (schema imports)

### Protobuf
- ✅ Messages, enums, services
- ✅ Nested types
- ✅ Imports
- ✅ Proto2 and Proto3

### JSON Schema
- ✅ Draft 7 and later
- ✅ Object, array, string, number, boolean, null
- ✅ Validation keywords
- ✅ $ref (references)

---

## Appendix B: Compatibility Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| BACKWARD | New schema can read old data | Add optional fields, remove fields |
| BACKWARD_TRANSITIVE | Compatible with all previous versions | Strict backward compat |
| FORWARD | Old schema can read new data | Deprecated readers |
| FORWARD_TRANSITIVE | All previous can read new | Strict forward compat |
| FULL | Both BACKWARD and FORWARD | Safest, most restrictive |
| FULL_TRANSITIVE | BACKWARD_TRANSITIVE + FORWARD_TRANSITIVE | Maximum safety |
| NONE | No validation | Development/testing only |

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Next Review:** 2025-12-13
**Owner:** Platform Engineering Team
