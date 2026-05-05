# 🛡️ Sports Media Detection Backend

REST API for detecting unauthorized use of official sports media across the internet. Built with **Node.js**, **Express**, and **Supabase** (PostgreSQL).

## What It Does

- **Register** official media assets with perceptual hash signatures
- **Ingest** detections from crawlers or AI pipelines
- **Auto-detect** unauthorized usage by checking against trusted sources
- **Auto-generate** alerts for suspicious activity in near real-time

## Project Structure

```
sol-challenge-backend/
├── config/
│   ├── env.js              # Environment variable loader + validation
│   ├── supabase.js         # Supabase client singleton
│   └── swagger.js          # OpenAPI 3.0 spec (centralized)
├── controllers/
│   ├── assetController.js  # Request handling for /assets
│   ├── detectionController.js # Request handling for /detections
│   ├── alertController.js  # Request handling for /alerts
│   └── sourceController.js # Request handling for /sources
├── db/
│   └── schema.sql          # Supabase/PostgreSQL table definitions
├── middleware/
│   └── errorHandler.js     # Global error handler
├── routes/
│   ├── index.js            # Central route registry
│   ├── assetRoutes.js
│   ├── detectionRoutes.js
│   ├── alertRoutes.js
│   └── sourceRoutes.js
├── services/
│   ├── assetService.js     # Database operations for assets
│   ├── detectionService.js # Detection ingestion + auto-authorization + alerts
│   ├── alertService.js     # Database operations for alerts
│   └── sourceService.js    # Trusted source management + domain matching
├── utils/
│   └── validators.js       # Zod validation schemas
├── server.js               # Express app entry point
├── .env.example            # Environment variable template
├── .gitignore
├── package.json
└── README.md
```

---

## Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Set Up Supabase

1. Create a project at [supabase.com](https://supabase.com)
2. Go to **SQL Editor** → paste the contents of `db/schema.sql` → **Run**
3. Copy your project URL and anon key from **Settings → API**

### 3. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your Supabase credentials:

```
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your-anon-key-here
PORT=3000
NODE_ENV=development
SIMILARITY_THRESHOLD=80
```

### 4. Start the Server

```bash
# Development (auto-restart on file changes)
npm run dev

# Production
npm start
```

The server starts at `http://localhost:3000`.

---

## API Endpoints

Base URL: `http://localhost:3000/api`

| Method | Endpoint               | Description                                      |
|--------|------------------------|--------------------------------------------------|
| GET    | `/health`              | Health check (no `/api` prefix)                  |
| POST   | `/api/assets`          | Register a new media asset                       |
| GET    | `/api/assets`          | List all registered media assets                 |
| GET    | `/api/assets/:id`      | Get asset by ID                                  |
| POST   | `/api/detections`      | Ingest a detection (auto-auth + auto-alert)      |
| GET    | `/api/detections`      | List detections with asset + source info         |
| GET    | `/api/detections/:id`  | Get detection by ID                              |
| POST   | `/api/alerts`          | Create a manual alert                            |
| GET    | `/api/alerts`          | List alerts with detection + asset info          |
| GET    | `/api/alerts/:id`      | Get alert by ID                                  |
| POST   | `/api/sources`         | Register a trusted source platform               |
| GET    | `/api/sources`         | List all trusted sources                         |

### Query Parameters

**GET /api/detections**
- `minScore` — filter detections with similarity_score ≥ value (e.g. `?minScore=80`)
- `isAuthorized` — filter by authorization (e.g. `?isAuthorized=false`)
- `limit` — max number of results (e.g. `?limit=10`)

**GET /api/alerts**
- `severity` — filter by severity: `low`, `medium`, `high`
- `limit` — max number of results

---

## Sample curl Requests

### Step 1: Register a Trusted Source

```bash
curl -X POST http://localhost:3000/api/sources \
  -H "Content-Type: application/json" \
  -d '{"platform_name": "YouTube", "url": "https://youtube.com"}'
```

### Step 2: Register an Asset

```bash
curl -X POST http://localhost:3000/api/assets \
  -H "Content-Type: application/json" \
  -d '{
    "name": "FIFA World Cup 2026 Official Logo",
    "type": "image",
    "hash_signature": "phash:a4f2e8b901c3d567",
    "owner": "FIFA"
  }'
```

### Step 3: Ingest a Detection (Unauthorized — triggers alert)

```bash
# Replace <ASSET_ID> with a real UUID from Step 2
curl -X POST http://localhost:3000/api/detections \
  -H "Content-Type: application/json" \
  -d '{
    "asset_id": "<ASSET_ID>",
    "source_url": "https://suspicious-site.com/images/logo.png",
    "similarity_score": 94.5
  }'
```

Response includes `alert_generated: true` and the auto-created alert.

### Step 4: Ingest a Detection (Authorized — no alert)

```bash
curl -X POST http://localhost:3000/api/detections \
  -H "Content-Type: application/json" \
  -d '{
    "asset_id": "<ASSET_ID>",
    "source_url": "https://youtube.com/watch?v=abc123",
    "similarity_score": 98.0
  }'
```

Response: `alert_generated: false` (source is trusted).

### Step 5: Fetch All Unauthorized Detections

```bash
curl "http://localhost:3000/api/detections?isAuthorized=false"
```

### Step 6: Fetch High Severity Alerts

```bash
curl "http://localhost:3000/api/alerts?severity=high"
```

### Step 7: Fetch All Alerts (with full context)

```bash
curl http://localhost:3000/api/alerts
```

---

## Near Real-Time Processing Flow

```
POST /api/detections
    │
    ▼
detectionService.create()
    │
    ├── Check source_url domain against sources table
    │   ├── Trusted?  → is_authorized = true
    │   └── Unknown?  → is_authorized = false
    │
    ├── Insert detection record
    │
    └── If (similarity_score ≥ threshold AND !is_authorized)
        │
        └── Auto-create alert with severity:
            ├── score ≥ 95  → high
            ├── score ≥ 90  → medium
            └── score ≥ 80  → low
```

---

## Database Schema

```
sources ─────────── (trusted platforms)
  id (PK)
  platform_name
  url (UNIQUE)
  created_at

assets ──────────── (registered media)
  id (PK)
  name
  type (image/video)
  hash_signature
  owner
  created_at

detections ─────────< alerts
  id (PK)              id (PK)
  asset_id (FK)        detection_id (FK)
  source_url           message
  similarity_score     severity (low/medium/high)
  is_authorized        created_at
  detected_at

Relationships:
  detections.asset_id    → assets.id (CASCADE delete)
  alerts.detection_id    → detections.id (CASCADE delete)
```

---

## Validation

All POST request bodies are validated using **Zod** schemas. Invalid input returns:

```json
{
  "success": false,
  "error": "Validation failed",
  "details": [
    "name is required",
    "type must be \"image\" or \"video\""
  ]
}
```

---

## Error Handling

All errors return a consistent JSON format:

```json
{
  "success": false,
  "error": "Descriptive error message"
}
```

| Status | Meaning                        |
|--------|--------------------------------|
| 400    | Validation / bad request       |
| 404    | Resource not found             |
| 409    | Conflict (duplicate)           |
| 429    | Rate limited                   |
| 500    | Internal server error          |

---

## API Documentation

Interactive Swagger UI is available at:

```
http://localhost:3000/api-docs
```

Raw OpenAPI JSON spec:

```
http://localhost:3000/api-docs.json
```

---

## Tech Stack

- **Runtime**: Node.js
- **Framework**: Express 5
- **Database**: Supabase (PostgreSQL)
- **Validation**: Zod
- **Security**: Helmet, CORS, Rate Limiting
- **Logging**: Morgan
- **Docs**: Swagger/OpenAPI 3.0
