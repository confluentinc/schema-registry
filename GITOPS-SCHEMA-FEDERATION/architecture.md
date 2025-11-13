# GitOps Schema Federation Manager - Architecture Specification

**Version:** 1.0
**Date:** 2025-11-13
**Status:** Design Complete

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Component Architecture](#component-architecture)
3. [Data Models](#data-models)
4. [API Specifications](#api-specifications)
5. [Deployment Architecture](#deployment-architecture)
6. [Technology Stack](#technology-stack)
7. [Sequence Diagrams](#sequence-diagrams)

---

## System Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Users / CI/CD                           │
└───────────────┬─────────────────────────────────┬───────────────┘
                │                                 │
         ┌──────▼──────┐                   ┌─────▼─────┐
         │  Web UI     │                   │  CLI Tool │
         │  (React)    │                   │  (Python) │
         └──────┬──────┘                   └─────┬─────┘
                │                                 │
                └─────────────┬───────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │   API Gateway      │
                    │   (FastAPI/Flask)  │
                    └─────────┬──────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
    ┌────▼────┐      ┌────────▼────────┐   ┌──────▼──────┐
    │ Schema  │      │  Git Manager    │   │  Deployment │
    │ ID      │      │  (GitPython)    │   │  Engine     │
    │ Service │      └────────┬────────┘   └──────┬──────┘
    └────┬────┘               │                    │
         │                    │                    │
    ┌────▼────┐      ┌────────▼────────┐   ┌──────▼──────┐
    │ SQLite/ │      │  SCHEMASTORE/   │   │  SR Client  │
    │ Postgres│      │  (Git Repo)     │   │  Library    │
    └─────────┘      └─────────────────┘   └──────┬──────┘
                                                   │
                 ┌─────────────────────────────────┼────────────────┐
                 │                                 │                │
         ┌───────▼────────┐              ┌─────────▼──────┐  ┌──────▼──────┐
         │ Schema Registry│              │ Schema Registry│  │  Confluent  │
         │   us-west      │              │  eu-central    │  │   Cloud SR  │
         │  (localhost:   │              │  (localhost:   │  │             │
         │   8081)        │              │   8082)        │  │             │
         └────────────────┘              └────────────────┘  └─────────────┘
```

### Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Web UI** | User interface for schema management, monitoring |
| **CLI Tool** | Command-line interface for automation |
| **API Gateway** | REST API for all operations, authentication, routing |
| **Schema ID Service** | Global ID allocation and tracking |
| **Git Manager** | Read/write operations to SCHEMASTORE repository |
| **Deployment Engine** | Multi-region schema deployment orchestration |
| **SR Client Library** | Abstraction layer for Schema Registry REST API |

---

## Component Architecture

### 1. Web UI (Frontend)

**Technology:** React 18+ with TypeScript

**Directory Structure:**
```
web-ui/
├── public/
│   └── index.html
├── src/
│   ├── components/
│   │   ├── Dashboard/
│   │   │   ├── Dashboard.tsx
│   │   │   ├── StatsCard.tsx
│   │   │   └── RecentActivity.tsx
│   │   ├── SchemaBrowser/
│   │   │   ├── SchemaTree.tsx
│   │   │   ├── SchemaViewer.tsx
│   │   │   └── SchemaSearch.tsx
│   │   ├── SchemaForm/
│   │   │   ├── AddSchemaForm.tsx
│   │   │   ├── CompatibilityCheck.tsx
│   │   │   └── RegionSelector.tsx
│   │   ├── RegistryManagement/
│   │   │   ├── RegistryList.tsx
│   │   │   ├── AddRegistryForm.tsx
│   │   │   └── ConnectionTest.tsx
│   │   ├── HealthDashboard/
│   │   │   ├── SyncStatus.tsx
│   │   │   ├── DriftDetection.tsx
│   │   │   └── AlertList.tsx
│   │   └── common/
│   │       ├── MonacoEditor.tsx
│   │       ├── LoadingSpinner.tsx
│   │       └── ErrorBoundary.tsx
│   ├── services/
│   │   ├── apiClient.ts
│   │   ├── schemaService.ts
│   │   ├── registryService.ts
│   │   └── gitService.ts
│   ├── hooks/
│   │   ├── useSchemas.ts
│   │   ├── useRegistries.ts
│   │   └── useHealthCheck.ts
│   ├── types/
│   │   ├── schema.ts
│   │   ├── registry.ts
│   │   └── deployment.ts
│   ├── App.tsx
│   └── index.tsx
├── package.json
└── tsconfig.json
```

**Key Libraries:**
- **React Router** - Navigation
- **Tanstack Query** - Data fetching and caching
- **Monaco Editor** - Code editor component
- **Tailwind CSS** - Styling
- **Axios** - HTTP client
- **Zod** - Runtime type validation
- **React Hook Form** - Form management

### 2. API Gateway (Backend)

**Technology:** Python FastAPI

**Directory Structure:**
```
api-service/
├── app/
│   ├── main.py
│   ├── config.py
│   ├── api/
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── schemas.py       # Schema CRUD endpoints
│   │   │   ├── schema_ids.py    # ID allocation endpoints
│   │   │   ├── registries.py    # SR connection management
│   │   │   ├── deployment.py    # Deployment endpoints
│   │   │   └── health.py        # Health check endpoints
│   ├── models/
│   │   ├── schema.py
│   │   ├── registry.py
│   │   ├── deployment.py
│   │   └── schema_id.py
│   ├── services/
│   │   ├── schema_id_service.py
│   │   ├── git_service.py
│   │   ├── deployment_service.py
│   │   ├── registry_client.py
│   │   └── compatibility_service.py
│   ├── repositories/
│   │   ├── schema_id_repository.py
│   │   └── schema_repository.py
│   └── utils/
│       ├── avro_parser.py
│       ├── protobuf_parser.py
│       ├── json_schema_parser.py
│       └── validators.py
├── tests/
│   ├── test_schema_api.py
│   ├── test_id_service.py
│   └── test_deployment.py
├── requirements.txt
└── Dockerfile
```

**Key Libraries:**
- **FastAPI** - Web framework
- **Pydantic** - Data validation
- **SQLAlchemy** - ORM for ID service database
- **GitPython** - Git operations
- **requests** - HTTP client for SR API
- **avro-python3** - Avro schema parsing
- **python-multipart** - File upload handling
- **pytest** - Testing framework

### 3. Schema ID Service

**Purpose:** Centralized global Schema ID allocation

**Storage:** SQLite (dev) / PostgreSQL (prod)

**Database Schema:**
```sql
CREATE TABLE schema_ids (
    id INTEGER PRIMARY KEY,
    schema_hash VARCHAR(64) UNIQUE NOT NULL,  -- SHA256 of canonical schema
    subject VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    context VARCHAR(255) DEFAULT 'default',
    schema_type VARCHAR(50) NOT NULL,         -- AVRO, PROTOBUF, JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    metadata JSONB,
    UNIQUE(context, subject, version)
);

CREATE INDEX idx_schema_hash ON schema_ids(schema_hash);
CREATE INDEX idx_subject ON schema_ids(subject);
CREATE INDEX idx_context ON schema_ids(context);
```

**API Endpoints:**

```python
POST /api/v1/schema-ids/allocate
Request:
{
  "subject": "user-events-value",
  "version": 1,
  "context": "default",
  "schemaType": "AVRO",
  "schemaContent": "{...}",  # Canonical form
  "metadata": {...}
}

Response:
{
  "id": 10001,
  "existing": false,
  "schemaHash": "a1b2c3...",
  "createdAt": "2025-11-13T10:00:00Z"
}

GET /api/v1/schema-ids/{id}
Response:
{
  "id": 10001,
  "subject": "user-events-value",
  "version": 1,
  "context": "default",
  "schemaType": "AVRO",
  "createdAt": "2025-11-13T10:00:00Z",
  "deployedRegions": ["us-west-local", "confluent-cloud"]
}

GET /api/v1/schema-ids?subject=user-events-value&version=1
Response:
{
  "schemas": [
    {
      "id": 10001,
      "subject": "user-events-value",
      "version": 1,
      ...
    }
  ]
}
```

**ID Allocation Algorithm:**
```python
def allocate_id(schema_content: str, subject: str, version: int, context: str) -> int:
    # 1. Compute canonical schema hash
    schema_hash = hashlib.sha256(canonicalize(schema_content).encode()).hexdigest()

    # 2. Check if schema already exists (idempotency)
    existing = db.query(SchemaID).filter_by(schema_hash=schema_hash).first()
    if existing:
        return existing.id, True  # Return existing ID

    # 3. Check if subject+version already has an ID
    existing_version = db.query(SchemaID).filter_by(
        subject=subject, version=version, context=context
    ).first()
    if existing_version:
        raise ConflictError("Subject/version already registered with different content")

    # 4. Allocate next available ID
    max_id = db.query(func.max(SchemaID.id)).scalar() or 10000
    new_id = max_id + 1

    # 5. Store in database
    schema_id_entry = SchemaID(
        id=new_id,
        schema_hash=schema_hash,
        subject=subject,
        version=version,
        context=context,
        ...
    )
    db.add(schema_id_entry)
    db.commit()

    return new_id, False
```

### 4. Git Manager

**Purpose:** Manage SCHEMASTORE repository

**Operations:**

```python
class GitManager:
    def __init__(self, repo_path: str):
        self.repo = git.Repo(repo_path)

    def add_schema(self, context: str, subject: str, version: int,
                   schema_content: str, metadata: dict) -> str:
        """
        Add new schema to SCHEMASTORE and commit
        Returns: commit SHA
        """
        # 1. Create directory structure
        schema_dir = f"SCHEMASTORE/{context}/{subject}/v{version}"
        os.makedirs(schema_dir, exist_ok=True)

        # 2. Write schema file
        schema_file = f"{schema_dir}/schema.avsc"
        with open(schema_file, 'w') as f:
            f.write(schema_content)

        # 3. Write metadata
        metadata_file = f"{schema_dir}/metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        # 4. Git add
        self.repo.index.add([schema_file, metadata_file])

        # 5. Commit
        commit_msg = f"Add {subject} v{version} (ID: {metadata['schemaId']})"
        commit = self.repo.index.commit(commit_msg)

        return commit.hexsha

    def get_schema(self, context: str, subject: str, version: int) -> dict:
        """Load schema and metadata from Git"""
        schema_dir = f"SCHEMASTORE/{context}/{subject}/v{version}"

        with open(f"{schema_dir}/schema.avsc") as f:
            schema_content = f.read()

        with open(f"{schema_dir}/metadata.json") as f:
            metadata = json.load(f)

        return {
            "schemaContent": schema_content,
            "metadata": metadata
        }

    def list_all_schemas(self) -> list:
        """Recursively scan SCHEMASTORE for all schemas"""
        schemas = []
        for context_dir in os.listdir("SCHEMASTORE"):
            context_path = f"SCHEMASTORE/{context_dir}"
            if not os.path.isdir(context_path):
                continue

            for subject_dir in os.listdir(context_path):
                subject_path = f"{context_path}/{subject_dir}"
                if not os.path.isdir(subject_path):
                    continue

                for version_dir in os.listdir(subject_path):
                    version_path = f"{subject_path}/{version_dir}"
                    if not os.path.isdir(version_path):
                        continue

                    # Load metadata
                    with open(f"{version_path}/metadata.json") as f:
                        metadata = json.load(f)

                    schemas.append({
                        "context": context_dir,
                        "subject": subject_dir,
                        "version": int(version_dir.replace("v", "")),
                        "metadata": metadata,
                        "path": version_path
                    })

        return schemas
```

### 5. Deployment Engine

**Purpose:** Deploy schemas to multiple Schema Registries

**Core Logic:**

```python
class DeploymentEngine:
    def __init__(self, config_path: str):
        self.config = self.load_config(config_path)
        self.registries = {
            name: SchemaRegistryClient(conf)
            for name, conf in self.config['regions'].items()
        }

    def deploy_schema(self, schema_path: str, dry_run: bool = False) -> DeploymentResult:
        """
        Deploy a single schema to all target regions
        """
        # 1. Load schema and metadata
        with open(f"{schema_path}/schema.avsc") as f:
            schema_content = f.read()
        with open(f"{schema_path}/metadata.json") as f:
            metadata = json.load(f)

        # 2. Determine target regions
        target_regions = metadata['deploymentRegions']

        # 3. Deploy to each region in parallel
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(
                    self._deploy_to_region,
                    region,
                    metadata['subject'],
                    schema_content,
                    metadata['schemaId'],
                    metadata['schemaType'],
                    dry_run
                ): region
                for region in target_regions
            }

            for future in as_completed(futures):
                region = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append(DeploymentResult(
                        region=region,
                        success=False,
                        error=str(e)
                    ))

        return DeploymentSummary(results)

    def _deploy_to_region(self, region: str, subject: str,
                          schema_content: str, schema_id: int,
                          schema_type: str, dry_run: bool) -> DeploymentResult:
        """
        Deploy to a single region
        """
        client = self.registries[region]

        if dry_run:
            logger.info(f"[DRY-RUN] Would deploy {subject} (ID: {schema_id}) to {region}")
            return DeploymentResult(region=region, success=True, dry_run=True)

        try:
            # 1. Check SR mode
            mode = client.get_mode()
            if mode != "IMPORT":
                raise ValueError(f"Registry {region} not in IMPORT mode (current: {mode})")

            # 2. Register schema with custom ID
            response = client.register_schema(
                subject=subject,
                schema=schema_content,
                schema_type=schema_type,
                schema_id=schema_id
            )

            # 3. Verify registration
            retrieved_schema = client.get_schema_by_id(schema_id)
            if retrieved_schema != schema_content:
                raise ValueError("Schema content mismatch after registration")

            return DeploymentResult(
                region=region,
                success=True,
                schema_id=schema_id,
                subject=subject
            )

        except Exception as e:
            logger.error(f"Failed to deploy to {region}: {e}")
            return DeploymentResult(
                region=region,
                success=False,
                error=str(e)
            )

    def deploy_all(self, dry_run: bool = False) -> dict:
        """
        Deploy all schemas in SCHEMASTORE
        """
        git_manager = GitManager(".")
        all_schemas = git_manager.list_all_schemas()

        results = {}
        for schema_info in all_schemas:
            schema_path = schema_info['path']
            result = self.deploy_schema(schema_path, dry_run)
            results[schema_path] = result

        return results
```

### 6. Schema Registry Client

**Purpose:** Abstraction layer for SR REST API

```python
class SchemaRegistryClient:
    def __init__(self, url: str, auth: dict = None):
        self.url = url.rstrip('/')
        self.session = requests.Session()

        if auth and auth['type'] == 'basic':
            self.session.auth = (auth['username'], auth['password'])

    def get_mode(self, subject: str = None) -> str:
        """Get registry mode"""
        endpoint = f"{self.url}/mode"
        if subject:
            endpoint += f"/{subject}"

        response = self.session.get(endpoint)
        response.raise_for_status()
        return response.json()['mode']

    def register_schema(self, subject: str, schema: str,
                        schema_type: str = "AVRO",
                        schema_id: int = None) -> dict:
        """Register schema with optional custom ID"""
        endpoint = f"{self.url}/subjects/{subject}/versions"

        payload = {
            "schema": schema,
            "schemaType": schema_type
        }

        if schema_id is not None:
            payload["id"] = schema_id

        response = self.session.post(endpoint, json=payload)
        response.raise_for_status()
        return response.json()

    def get_schema_by_id(self, schema_id: int) -> str:
        """Get schema content by ID"""
        endpoint = f"{self.url}/schemas/ids/{schema_id}"
        response = self.session.get(endpoint)
        response.raise_for_status()
        return response.json()['schema']

    def list_subjects(self) -> list:
        """List all subjects"""
        endpoint = f"{self.url}/subjects"
        response = self.session.get(endpoint)
        response.raise_for_status()
        return response.json()

    def get_versions(self, subject: str) -> list:
        """Get all versions for a subject"""
        endpoint = f"{self.url}/subjects/{subject}/versions"
        response = self.session.get(endpoint)
        response.raise_for_status()
        return response.json()

    def test_connection(self) -> dict:
        """Test connectivity and return SR info"""
        try:
            # Test basic connectivity
            response = self.session.get(f"{self.url}/schemas/types", timeout=5)
            response.raise_for_status()

            # Get mode
            mode = self.get_mode()

            return {
                "success": True,
                "mode": mode,
                "schemaTypes": response.json()
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
```

---

## Data Models

### Schema Model

```python
from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum

class SchemaType(str, Enum):
    AVRO = "AVRO"
    PROTOBUF = "PROTOBUF"
    JSON = "JSON"

class CompatibilityMode(str, Enum):
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"

class SchemaMetadata(BaseModel):
    schemaId: int
    subject: str
    version: int
    context: str = "default"
    schemaType: SchemaType
    compatibilityMode: CompatibilityMode
    deploymentRegions: List[str]
    createdAt: str
    createdBy: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)

class SchemaDefinition(BaseModel):
    metadata: SchemaMetadata
    schemaContent: str

class AddSchemaRequest(BaseModel):
    subject: str
    context: str = "default"
    schemaType: SchemaType
    schemaContent: str
    compatibilityMode: CompatibilityMode = CompatibilityMode.BACKWARD
    deploymentRegions: List[str]
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
```

### Registry Model

```python
class AuthType(str, Enum):
    NONE = "none"
    BASIC = "basic"
    SSL = "ssl"

class RegistryConfig(BaseModel):
    name: str
    url: str
    authType: AuthType
    username: Optional[str] = None
    password: Optional[str] = None
    mode: Optional[str] = None  # Filled after test connection

class RegistryTestResult(BaseModel):
    success: bool
    mode: Optional[str] = None
    schemaTypes: Optional[List[str]] = None
    error: Optional[str] = None
```

### Deployment Model

```python
class DeploymentResult(BaseModel):
    region: str
    success: bool
    schema_id: Optional[int] = None
    subject: Optional[str] = None
    error: Optional[str] = None
    dry_run: bool = False

class DeploymentSummary(BaseModel):
    total: int
    successful: int
    failed: int
    results: List[DeploymentResult]
```

---

## API Specifications

### OpenAPI / Swagger Schema

```yaml
openapi: 3.0.0
info:
  title: GitOps Schema Federation Manager API
  version: 1.0.0
  description: API for managing Kafka schemas across multiple registries

servers:
  - url: http://localhost:8080/api/v1
    description: Local development

paths:
  /schemas:
    get:
      summary: List all schemas
      parameters:
        - name: context
          in: query
          schema:
            type: string
        - name: subject
          in: query
          schema:
            type: string
      responses:
        200:
          description: List of schemas
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/SchemaDefinition'

    post:
      summary: Add new schema
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AddSchemaRequest'
      responses:
        201:
          description: Schema created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SchemaMetadata'
        400:
          description: Validation error
        409:
          description: Compatibility conflict

  /schemas/{context}/{subject}/{version}:
    get:
      summary: Get specific schema version
      parameters:
        - name: context
          in: path
          required: true
          schema:
            type: string
        - name: subject
          in: path
          required: true
          schema:
            type: string
        - name: version
          in: path
          required: true
          schema:
            type: integer
      responses:
        200:
          description: Schema details
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SchemaDefinition'

  /schema-ids/allocate:
    post:
      summary: Allocate global schema ID
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                subject:
                  type: string
                version:
                  type: integer
                context:
                  type: string
                schemaType:
                  type: string
                schemaContent:
                  type: string
      responses:
        200:
          description: ID allocated
          content:
            application/json:
              schema:
                type: object
                properties:
                  id:
                    type: integer
                  existing:
                    type: boolean

  /registries:
    get:
      summary: List all configured registries
      responses:
        200:
          description: List of registries
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/RegistryConfig'

    post:
      summary: Add new registry connection
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/RegistryConfig'
      responses:
        201:
          description: Registry added

  /registries/test:
    post:
      summary: Test registry connection
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/RegistryConfig'
      responses:
        200:
          description: Test result
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/RegistryTestResult'

  /deployment/deploy:
    post:
      summary: Deploy schemas
      parameters:
        - name: dry_run
          in: query
          schema:
            type: boolean
            default: false
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                subjects:
                  type: array
                  items:
                    type: string
                regions:
                  type: array
                  items:
                    type: string
      responses:
        200:
          description: Deployment results
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/DeploymentSummary'

  /health/sync-status:
    get:
      summary: Get schema synchronization status
      responses:
        200:
          description: Sync status
          content:
            application/json:
              schema:
                type: object
                properties:
                  totalSchemas:
                    type: integer
                  inSync:
                    type: integer
                  drift:
                    type: integer
                  missing:
                    type: integer
                  extra:
                    type: integer

components:
  schemas:
    SchemaType:
      type: string
      enum: [AVRO, PROTOBUF, JSON]

    CompatibilityMode:
      type: string
      enum: [BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE]

    SchemaMetadata:
      type: object
      properties:
        schemaId:
          type: integer
        subject:
          type: string
        version:
          type: integer
        context:
          type: string
        schemaType:
          $ref: '#/components/schemas/SchemaType'
        compatibilityMode:
          $ref: '#/components/schemas/CompatibilityMode'
        deploymentRegions:
          type: array
          items:
            type: string
        createdAt:
          type: string
          format: date-time
        createdBy:
          type: string
        description:
          type: string
        tags:
          type: array
          items:
            type: string

    # ... (other schemas)
```

---

## Deployment Architecture

### Docker Compose Setup

**File:** `docker-compose.yml`

```yaml
version: '3.8'

services:
  # Cluster 1: US West
  kafka-us-west:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka-us-west
    container_name: kafka-us-west
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-us-west:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

  schema-registry-us-west:
    image: confluentinc/cp-schema-registry:7.6.0
    hostname: schema-registry-us-west
    container_name: schema-registry-us-west
    depends_on:
      - kafka-us-west
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry-us-west
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-us-west:9092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_MODE_MUTABILITY: "true"

  control-center-us-west:
    image: confluentinc/cp-enterprise-control-center:7.6.0
    hostname: control-center-us-west
    container_name: control-center-us-west
    depends_on:
      - kafka-us-west
      - schema-registry-us-west
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: kafka-us-west:9092
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: http://schema-registry-us-west:8081
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1

  # Cluster 2: EU Central
  kafka-eu-central:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka-eu-central
    container_name: kafka-eu-central
    ports:
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-eu-central:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

  schema-registry-eu-central:
    image: confluentinc/cp-schema-registry:7.6.0
    hostname: schema-registry-eu-central
    container_name: schema-registry-eu-central
    depends_on:
      - kafka-eu-central
    ports:
      - "8082:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry-eu-central
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-eu-central:9093
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_MODE_MUTABILITY: "true"

  control-center-eu-central:
    image: confluentinc/cp-enterprise-control-center:7.6.0
    hostname: control-center-eu-central
    container_name: control-center-eu-central
    depends_on:
      - kafka-eu-central
      - schema-registry-eu-central
    ports:
      - "9022:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: kafka-eu-central:9093
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: http://schema-registry-eu-central:8081
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1

  # GitOps Schema Federation Manager
  gitops-schema-api:
    build:
      context: ./api-service
      dockerfile: Dockerfile
    container_name: gitops-schema-api
    ports:
      - "8080:8080"
    volumes:
      - ./SCHEMASTORE:/app/SCHEMASTORE
      - ./deployment-config.yaml:/app/deployment-config.yaml
    environment:
      DATABASE_URL: sqlite:///data/schema_ids.db
      GIT_REPO_PATH: /app
      LOG_LEVEL: INFO
    depends_on:
      - schema-registry-us-west
      - schema-registry-eu-central

  gitops-schema-ui:
    build:
      context: ./web-ui
      dockerfile: Dockerfile
    container_name: gitops-schema-ui
    ports:
      - "3000:3000"
    environment:
      REACT_APP_API_URL: http://localhost:8080/api/v1
    depends_on:
      - gitops-schema-api

networks:
  default:
    name: schema-federation-network
```

---

## Technology Stack

### Frontend
- **Framework:** React 18+
- **Language:** TypeScript 5+
- **Styling:** Tailwind CSS
- **State Management:** Tanstack Query (React Query)
- **Routing:** React Router v6
- **Forms:** React Hook Form + Zod
- **Code Editor:** Monaco Editor (VS Code component)
- **HTTP Client:** Axios
- **Build Tool:** Vite

### Backend
- **Framework:** FastAPI (Python 3.10+)
- **ORM:** SQLAlchemy
- **Database:** SQLite (dev) / PostgreSQL (prod)
- **Git:** GitPython
- **Schema Parsing:** avro-python3, protobuf
- **Testing:** pytest, pytest-asyncio
- **API Docs:** Swagger UI (auto-generated by FastAPI)

### Infrastructure
- **Containerization:** Docker & Docker Compose
- **CI/CD:** GitHub Actions / GitLab CI
- **Monitoring:** Prometheus + Grafana (optional)
- **Logging:** Python logging + JSON formatter

---

## Sequence Diagrams

### Add Schema Flow

```
User → Web UI: Fill add schema form
Web UI → API: POST /api/v1/schemas
API → Compatibility Service: Validate compatibility
Compatibility Service → Git Manager: Get previous version
Git Manager → SCHEMASTORE: Read v1 schema
Git Manager → Compatibility Service: Return previous schema
Compatibility Service → API: ✓ Compatible
API → Schema ID Service: POST /schema-ids/allocate
Schema ID Service → Database: Check existing hash
Database → Schema ID Service: Not found
Schema ID Service → Database: INSERT new ID (10001)
Schema ID Service → API: Return ID 10001
API → Git Manager: Add schema to SCHEMASTORE
Git Manager → SCHEMASTORE: Create v2/schema.avsc
Git Manager → SCHEMASTORE: Create v2/metadata.json
Git Manager → Git: Commit changes
Git Manager → API: Return commit SHA
API → Web UI: 201 Created {schemaId: 10001}
Web UI → User: Success message
```

### Deploy Schema Flow

```
CI/CD → API: POST /api/v1/deployment/deploy
API → Git Manager: List all schemas
Git Manager → SCHEMASTORE: Scan folders
Git Manager → API: Return schema list
API → Deployment Engine: Deploy all schemas
Deployment Engine → Config: Load deployment-config.yaml
Deployment Engine → Parallel:
  Thread 1 → SR us-west: POST /subjects/user-events-value/versions {id: 10001}
  Thread 2 → SR eu-central: POST /subjects/user-events-value/versions {id: 10001}
  Thread 3 → SR Confluent Cloud: POST /subjects/user-events-value/versions {id: 10001}
SR us-west → Thread 1: 200 OK {id: 10001}
SR eu-central → Thread 2: 200 OK {id: 10001}
SR Confluent Cloud → Thread 3: 200 OK {id: 10001}
Deployment Engine → API: Return summary (3/3 successful)
API → CI/CD: 200 OK {total: 3, successful: 3, failed: 0}
```

### Health Check Flow

```
User → Web UI: Open health dashboard
Web UI → API: GET /api/v1/health/sync-status
API → Git Manager: Load all schemas from SCHEMASTORE
Git Manager → API: Return Git schemas (150 schemas)
API → Parallel:
  Thread 1 → SR us-west: GET /subjects (list all)
  Thread 2 → SR eu-central: GET /subjects (list all)
  Thread 3 → SR Confluent Cloud: GET /subjects (list all)
SR us-west → Thread 1: [user-events-value, order-events-value, ...]
SR eu-central → Thread 2: [user-events-value, gdpr-events-value, ...]
SR Confluent Cloud → Thread 3: [user-events-value, order-events-value, ...]
API → Comparison Logic:
  - Compare Git vs us-west: ✓ In sync (100 schemas)
  - Compare Git vs eu-central: ⚠ 2 missing, 1 drift
  - Compare Git vs Confluent Cloud: ⚠ 5 missing
API → Web UI: Return sync status
Web UI → User: Display health dashboard with alerts
```

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Next Review:** 2025-12-13
**Owner:** Platform Engineering Team
