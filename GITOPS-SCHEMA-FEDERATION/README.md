# GitOps Schema Federation Manager

A comprehensive solution for managing Kafka schema deployment across multiple Schema Registry instances with globally coordinated Schema IDs.

---

## 🎯 Overview

GitOps Schema Federation Manager enables:

- **GitOps Workflow**: All schemas versioned in Git as the single source of truth
- **Global Schema ID Management**: Centralized ID allocation preventing conflicts across regions
- **Multi-Region Deployment**: Deploy schemas selectively to relevant regions
- **Compatibility Validation**: Pre-deployment compatibility checks
- **Web UI**: User-friendly interface for schema management
- **Health Monitoring**: Real-time synchronization status and drift detection

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [requirements.md](./requirements.md) | Complete functional and non-functional requirements |
| [architecture.md](./architecture.md) | System architecture, components, and API specifications |
| [test-setup.md](./test-setup.md) | Step-by-step test environment setup (Phases 1-3) |
| [implementation-guide.md](./implementation-guide.md) | Implementation roadmap and development guide |

---

## 🚀 Quick Start

### Prerequisites

```bash
# Required software
docker --version          # >= 20.10
docker-compose --version  # >= 2.0
git --version             # >= 2.30
python3 --version         # >= 3.10
node --version            # >= 18.0
```

### Phase 1: Single Schema Registry Test

```bash
# 1. Start test infrastructure
docker-compose -f docker-compose-phase1.yml up -d

# 2. Wait for services (30-60 seconds)
sleep 60

# 3. Set Schema Registry to IMPORT mode
curl -X PUT http://localhost:8081/mode \
  -H "Content-Type: application/json" \
  --data '{"mode":"IMPORT"}'

# 4. Verify setup
curl http://localhost:8081/mode
# Expected: {"mode":"IMPORT"}

# 5. Deploy example schema
./deployment-scripts/deploy.sh --config deployment-config-phase1.yaml

# 6. Verify deployment
curl http://localhost:8081/subjects | jq .
```

### Start API Service

```bash
cd api-service
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080
```

### Start Web UI

```bash
cd web-ui
npm install
npm run dev
# Open http://localhost:3000
```

---

## 📁 Project Structure

```
GITOPS-SCHEMA-FEDERATION/
├── README.md                    # This file
├── requirements.md              # Requirements specification
├── architecture.md              # Architecture documentation
├── test-setup.md                # Test environment setup guide
├── implementation-guide.md      # Implementation roadmap
│
├── api-service/                 # Backend API service (Python FastAPI)
│   ├── app/
│   │   ├── main.py
│   │   ├── api/v1/             # API endpoints
│   │   ├── models/             # Data models
│   │   ├── services/           # Business logic
│   │   └── utils/              # Utilities
│   ├── requirements.txt
│   ├── Dockerfile
│   └── README.md
│
├── web-ui/                      # Frontend web application (React)
│   ├── src/
│   │   ├── components/         # React components
│   │   ├── services/           # API clients
│   │   ├── hooks/              # Custom React hooks
│   │   └── types/              # TypeScript types
│   ├── package.json
│   ├── Dockerfile
│   └── README.md
│
├── deployment-scripts/          # Deployment automation
│   ├── deploy.sh               # Main deployment script
│   ├── validate-schemas.sh     # Schema validation script
│   └── health-check.sh         # Health check script
│
├── schemas-example/             # Example schema repository structure
│   └── default/
│       └── user-events-value/
│           └── v1/
│               ├── schema.avsc
│               └── metadata.json
│
├── docker-compose-phase1.yml    # Single SR test setup
├── docker-compose-phase2.yml    # Multi-region test setup
├── docker-compose.yml           # Full production-like setup
│
├── deployment-config.yaml       # Deployment configuration
└── .env.example                 # Environment variables template
```

---

## 🗂️ Schema Store Structure

Schemas are organized in Git following this pattern:

```
SCHEMASTORE/
├── {context}/
│   └── {subject}/
│       └── v{version}/
│           ├── schema.avsc      # Avro schema
│           │   OR schema.proto  # Protobuf schema
│           │   OR schema.json   # JSON Schema
│           └── metadata.json    # Schema metadata
```

### Example: `SCHEMASTORE/default/user-events-value/v1/metadata.json`

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
  "createdBy": "platform-team@example.com",
  "description": "User event payload schema",
  "tags": ["events", "users"]
}
```

---

## 🔧 Configuration

### Deployment Config

**File:** `deployment-config.yaml`

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

  - context: default
    subjects: ["gdpr-*"]
    regions: ["eu-central-local", "confluent-cloud"]
```

### Environment Variables

**File:** `.env`

```bash
# Schema ID Service Database
DATABASE_URL=sqlite:///data/schema_ids.db

# Git Configuration
GIT_REPO_PATH=/path/to/repo
GIT_COMMIT_AUTHOR=GitOps Schema Manager <gitops@example.com>

# Confluent Cloud (if using)
CCLOUD_SR_API_KEY=your_api_key
CCLOUD_SR_API_SECRET=your_api_secret
CCLOUD_SR_URL=https://psrc-xxxxx.us-east-2.aws.confluent.cloud

# API Service
API_PORT=8080
LOG_LEVEL=INFO

# Web UI
REACT_APP_API_URL=http://localhost:8080/api/v1
```

---

## 🛠️ Usage

### Add New Schema (CLI)

```bash
# Via API
curl -X POST http://localhost:8080/api/v1/schemas \
  -H "Content-Type: application/json" \
  --data '{
    "subject": "order-events-value",
    "context": "default",
    "schemaType": "AVRO",
    "schemaContent": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"}]}",
    "compatibilityMode": "BACKWARD",
    "deploymentRegions": ["us-west-local"],
    "description": "Order event schema"
  }'

# Response
{
  "schemaId": 10002,
  "subject": "order-events-value",
  "version": 1,
  ...
}
```

### Deploy Schemas

```bash
# Deploy all schemas to all regions
./deployment-scripts/deploy.sh

# Dry run
./deployment-scripts/deploy.sh --dry-run

# Deploy specific subject
./deployment-scripts/deploy.sh --subject user-events-value

# Deploy to specific region
./deployment-scripts/deploy.sh --region us-west-local

# Verbose output
./deployment-scripts/deploy.sh --verbose
```

### Health Check

```bash
# Via API
curl http://localhost:8080/api/v1/health/sync-status | jq .

# Via Web UI
open http://localhost:3000/health
```

---

## 📊 Web UI Features

### Dashboard
- Total schemas count
- Schemas by context/subject
- Recent schema changes
- Deployment status overview

### Schema Browser
- Tree view: Context → Subject → Versions
- Search and filter capabilities
- Syntax-highlighted schema viewing
- Download schema files

### Add New Schema
- Upload or paste schema content
- Real-time validation feedback
- Compatibility check results
- Automatic ID allocation

### Schema Registry Connections
- Add/edit/delete SR connections
- Test connectivity
- View current mode

### Health Dashboard
- Per-region deployment status
- Git vs deployed schema comparison
- Missing/drift/conflict alerts
- One-click re-deployment

---

## 🧪 Testing

### Unit Tests

```bash
# API Service
cd api-service
pytest tests/ --cov=app --cov-report=html

# Web UI
cd web-ui
npm test -- --coverage
```

### Integration Tests

```bash
# Start test infrastructure
docker-compose -f docker-compose-phase1.yml up -d

# Run integration tests
pytest tests/integration/ -v

# Cleanup
docker-compose -f docker-compose-phase1.yml down -v
```

### End-to-End Test

```bash
# 1. Add schema via UI/API
# 2. Verify ID allocated
# 3. Deploy to test SR
# 4. Verify schema registered with correct ID
# 5. Check health dashboard shows sync
```

---

## 🐛 Troubleshooting

### Schema Registry won't start

```bash
# Check logs
docker logs schema-registry-test

# Common fix: Restart Kafka first
docker-compose restart kafka
sleep 30
docker-compose restart schema-registry
```

### Mode change fails

```bash
# Use force flag
curl -X PUT "http://localhost:8081/mode?force=true" \
  -H "Content-Type: application/json" \
  --data '{"mode":"IMPORT"}'
```

### Deployment fails with ID conflict

```bash
# Check existing schema
curl http://localhost:8081/schemas/ids/10001 | jq .

# Verify you're using correct schema content
# Use different ID if intentional
```

### API service connection error

```bash
# Check API is running
curl http://localhost:8080/health

# Check CORS settings if from browser
# Verify REACT_APP_API_URL in web-ui/.env
```

---

## 📋 Implementation Phases

### Phase 1: Core Infrastructure (Weeks 1-2)
- ✅ Schema ID Service (SQLite backend)
- ✅ Basic API endpoints (schemas, IDs)
- ✅ Git Manager for SCHEMASTORE operations
- ✅ Deployment script (shell)

### Phase 2: Web UI (Weeks 3-4)
- 🔄 React application setup
- 🔄 Schema browser component
- 🔄 Add schema form with validation
- 🔄 Registry connection management

### Phase 3: Advanced Features (Weeks 5-6)
- ⏳ Health dashboard with drift detection
- ⏳ Compatibility validation service
- ⏳ CI/CD pipeline (GitHub Actions)
- ⏳ Monitoring and logging

### Phase 4: Production Readiness (Weeks 7-8)
- ⏳ PostgreSQL migration
- ⏳ Authentication and authorization
- ⏳ Performance testing
- ⏳ Documentation and training

---

## 🤝 Contributing

### Development Workflow

1. Create feature branch: `git checkout -b feature/schema-validation`
2. Make changes with tests
3. Run linting: `pytest && npm test`
4. Commit: `git commit -m "Add schema validation"`
5. Push: `git push origin feature/schema-validation`
6. Create Pull Request

### Code Standards

**Python:**
- PEP 8 style guide
- Type hints required
- Docstrings for public functions
- Test coverage > 80%

**TypeScript/React:**
- ESLint + Prettier
- Functional components with hooks
- PropTypes or TypeScript interfaces
- Test coverage > 70%

---

## 📝 License

Copyright © 2025 Platform Engineering Team

---

## 🆘 Support

- **Documentation**: See `docs/` directory
- **Issues**: Create GitHub issue
- **Slack**: #schema-federation channel
- **Email**: platform-team@example.com

---

## 🗺️ Roadmap

**Q1 2025:**
- ✅ Phase 1: Core infrastructure
- 🔄 Phase 2: Web UI
- ⏳ Phase 3: Advanced features

**Q2 2025:**
- Production deployment
- Multi-tenancy support
- Schema lineage visualization
- Advanced RBAC

**Q3 2025:**
- Schema marketplace
- AI-powered compatibility suggestions
- Real-time bi-directional sync
- Performance optimization

---

**Version:** 1.0.0
**Last Updated:** 2025-11-13
**Maintained By:** Platform Engineering Team
