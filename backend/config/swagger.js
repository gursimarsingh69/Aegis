const swaggerJsdoc = require('swagger-jsdoc');

const options = {
  definition: {
    openapi: '3.0.3',
    info: {
      title: 'Sports Media Detection API',
      version: '1.0.0',
      description:
        'Backend API for detecting unauthorized use of official sports media across the internet. Register assets, ingest detections from crawlers/AI pipelines, auto-detect unauthorized usage, and generate alerts — all in near real-time.',
      contact: {
        name: 'API Support',
      },
      license: {
        name: 'ISC',
      },
    },
    servers: [
      {
        url: 'http://localhost:3000',
        description: 'Local development',
      },
    ],
    tags: [
      { name: 'Health', description: 'Server health check' },
      { name: 'Assets', description: 'Register and manage official media assets' },
      { name: 'Detections', description: 'Ingest and query media usage detections' },
      { name: 'Alerts', description: 'View and manage unauthorized usage alerts' },
      { name: 'Sources', description: 'Manage trusted platform sources' },
    ],

    // ── All Path Definitions ─────────────────────────────────────────────
    paths: {

      // ── Health ──────────────────────────────────────────────────────────
      '/health': {
        get: {
          summary: 'Health check',
          tags: ['Health'],
          description: 'Returns server status, uptime, and current timestamp.',
          responses: {
            200: {
              description: 'Server is healthy',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  status:    { type: 'string', example: 'ok' },
                  uptime:    { type: 'number', example: 123.456 },
                  timestamp: { type: 'string', format: 'date-time' },
                },
              }}},
            },
          },
        },
      },

      // ── Assets ─────────────────────────────────────────────────────────
      '/api/assets': {
        get: {
          summary: 'List all registered media assets',
          tags: ['Assets'],
          description: 'Returns all assets ordered by creation date (newest first).',
          responses: {
            200: {
              description: 'A list of assets',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { type: 'array', items: { $ref: '#/components/schemas/Asset' } },
                },
              }}},
            },
            500: { description: 'Server error', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
          },
        },
        post: {
          summary: 'Register a new media asset',
          tags: ['Assets'],
          description: 'Register an official media asset with its metadata and perceptual hash signature for identification.',
          requestBody: { required: true, content: { 'application/json': { schema: { $ref: '#/components/schemas/AssetInput' } } } },
          responses: {
            201: {
              description: 'Asset registered successfully',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { $ref: '#/components/schemas/Asset' },
                },
              }}},
            },
            400: { description: 'Validation failed', content: { 'application/json': { schema: { $ref: '#/components/schemas/ValidationError' } } } },
          },
        },
      },
      '/api/assets/{id}': {
        get: {
          summary: 'Get asset by ID',
          tags: ['Assets'],
          description: 'Returns a single asset by its UUID.',
          parameters: [{ in: 'path', name: 'id', required: true, schema: { type: 'string', format: 'uuid' }, description: 'Asset UUID' }],
          responses: {
            200: {
              description: 'Asset found',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { $ref: '#/components/schemas/Asset' },
                },
              }}},
            },
            404: { description: 'Asset not found', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
          },
        },
      },

      // ── Detections ─────────────────────────────────────────────────────
      '/api/detections': {
        get: {
          summary: 'List all detections with asset details',
          tags: ['Detections'],
          description: 'Returns all detections with their related asset info. Supports optional filtering by similarity score, authorization status, and limit.',
          parameters: [
            { in: 'query', name: 'minScore', schema: { type: 'number', format: 'float' }, description: 'Filter detections with similarity_score ≥ this value' },
            { in: 'query', name: 'isAuthorized', schema: { type: 'boolean' }, description: 'Filter by authorization status (true/false)' },
            { in: 'query', name: 'limit', schema: { type: 'integer' }, description: 'Maximum number of results to return' },
          ],
          responses: {
            200: {
              description: 'A list of detections with nested asset data',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { type: 'array', items: { $ref: '#/components/schemas/Detection' } },
                },
              }}},
            },
            500: { description: 'Server error', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
          },
        },
        post: {
          summary: 'Ingest a new detection (simulates crawler/AI pipeline)',
          tags: ['Detections'],
          description: 'Creates a new detection record. The system automatically: (1) checks if the source URL belongs to a trusted platform and sets is_authorized, (2) if similarity_score ≥ threshold AND unauthorized → auto-generates an alert.',
          requestBody: { required: true, content: { 'application/json': { schema: { $ref: '#/components/schemas/DetectionInput' } } } },
          responses: {
            201: {
              description: 'Detection created (with optional auto-generated alert)',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: {
                    type: 'object',
                    properties: {
                      detection: { $ref: '#/components/schemas/Detection' },
                      alert_generated: { type: 'boolean', example: true },
                      alert: { $ref: '#/components/schemas/Alert' },
                    },
                  },
                },
              }}},
            },
            400: { description: 'Validation failed', content: { 'application/json': { schema: { $ref: '#/components/schemas/ValidationError' } } } },
          },
        },
      },
      '/api/detections/{id}': {
        get: {
          summary: 'Get detection by ID',
          tags: ['Detections'],
          description: 'Returns a single detection with its related asset details.',
          parameters: [{ in: 'path', name: 'id', required: true, schema: { type: 'string', format: 'uuid' }, description: 'Detection UUID' }],
          responses: {
            200: {
              description: 'Detection found',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { $ref: '#/components/schemas/Detection' },
                },
              }}},
            },
            404: { description: 'Detection not found', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
          },
        },
      },

      // ── Alerts ─────────────────────────────────────────────────────────
      '/api/alerts': {
        get: {
          summary: 'List all alerts with detection and asset info',
          tags: ['Alerts'],
          description: 'Returns all alerts with their related detection and asset data. Supports optional filtering by severity level.',
          parameters: [
            { in: 'query', name: 'severity', schema: { type: 'string', enum: ['low', 'medium', 'high'] }, description: 'Filter by severity level' },
            { in: 'query', name: 'limit', schema: { type: 'integer' }, description: 'Maximum number of results to return' },
          ],
          responses: {
            200: {
              description: 'A list of alerts with nested detection and asset data',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { type: 'array', items: { $ref: '#/components/schemas/Alert' } },
                },
              }}},
            },
            400: { description: 'Invalid severity filter', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
            500: { description: 'Server error', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
          },
        },
        post: {
          summary: 'Create a manual alert',
          tags: ['Alerts'],
          description: 'Manually creates an alert linked to an existing detection. Most alerts are auto-generated by the detection pipeline, but this endpoint allows manual creation.',
          requestBody: { required: true, content: { 'application/json': { schema: { $ref: '#/components/schemas/AlertInput' } } } },
          responses: {
            201: {
              description: 'Alert created successfully',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { $ref: '#/components/schemas/Alert' },
                },
              }}},
            },
            400: { description: 'Validation failed', content: { 'application/json': { schema: { $ref: '#/components/schemas/ValidationError' } } } },
          },
        },
      },
      '/api/alerts/{id}': {
        get: {
          summary: 'Get alert by ID',
          tags: ['Alerts'],
          description: 'Returns a single alert with its related detection and asset details.',
          parameters: [{ in: 'path', name: 'id', required: true, schema: { type: 'string', format: 'uuid' }, description: 'Alert UUID' }],
          responses: {
            200: {
              description: 'Alert found',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { $ref: '#/components/schemas/Alert' },
                },
              }}},
            },
            404: { description: 'Alert not found', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
          },
        },
      },

      // ── Sources ────────────────────────────────────────────────────────
      '/api/sources': {
        get: {
          summary: 'List all trusted sources',
          tags: ['Sources'],
          description: 'Returns all registered trusted platform sources.',
          responses: {
            200: {
              description: 'A list of trusted sources',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { type: 'array', items: { $ref: '#/components/schemas/Source' } },
                },
              }}},
            },
            500: { description: 'Server error', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
          },
        },
        post: {
          summary: 'Register a trusted source platform',
          tags: ['Sources'],
          description: 'Registers a new trusted source. When a detection\'s source_url hostname matches a trusted source, it is automatically marked as authorized.',
          requestBody: { required: true, content: { 'application/json': { schema: { $ref: '#/components/schemas/SourceInput' } } } },
          responses: {
            201: {
              description: 'Source registered successfully',
              content: { 'application/json': { schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean', example: true },
                  data: { $ref: '#/components/schemas/Source' },
                },
              }}},
            },
            400: { description: 'Validation failed', content: { 'application/json': { schema: { $ref: '#/components/schemas/ValidationError' } } } },
            409: { description: 'Source URL already exists', content: { 'application/json': { schema: { $ref: '#/components/schemas/ErrorResponse' } } } },
          },
        },
      },
    },

    // ── Reusable Schemas ─────────────────────────────────────────────────
    components: {
      schemas: {
        Asset: {
          type: 'object',
          properties: {
            id:             { type: 'string', format: 'uuid', example: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' },
            name:           { type: 'string', example: 'FIFA World Cup 2026 Official Logo' },
            type:           { type: 'string', enum: ['image', 'video'], example: 'image' },
            hash_signature: { type: 'string', example: 'phash:a4f2e8b901c3d567' },
            owner:          { type: 'string', example: 'FIFA' },
            created_at:     { type: 'string', format: 'date-time', example: '2026-04-27T12:00:00Z' },
          },
        },
        AssetInput: {
          type: 'object',
          required: ['name', 'type', 'hash_signature', 'owner'],
          properties: {
            name:           { type: 'string', example: 'FIFA World Cup 2026 Official Logo' },
            type:           { type: 'string', enum: ['image', 'video'], example: 'image' },
            hash_signature: { type: 'string', example: 'phash:a4f2e8b901c3d567' },
            owner:          { type: 'string', example: 'FIFA' },
          },
        },
        Detection: {
          type: 'object',
          properties: {
            id:               { type: 'string', format: 'uuid' },
            asset_id:         { type: 'string', format: 'uuid' },
            source_url:       { type: 'string', format: 'uri', example: 'https://suspicious-site.com/images/logo.png' },
            detected_at:      { type: 'string', format: 'date-time' },
            similarity_score: { type: 'number', format: 'float', example: 94.5 },
            is_authorized:    { type: 'boolean', example: false },
            assets:           { $ref: '#/components/schemas/Asset' },
          },
        },
        DetectionInput: {
          type: 'object',
          required: ['asset_id', 'source_url', 'similarity_score'],
          properties: {
            asset_id:         { type: 'string', format: 'uuid', example: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' },
            source_url:       { type: 'string', format: 'uri', example: 'https://suspicious-site.com/images/logo.png' },
            similarity_score: { type: 'number', format: 'float', minimum: 0, maximum: 100, example: 94.5 },
          },
        },
        Alert: {
          type: 'object',
          properties: {
            id:           { type: 'string', format: 'uuid' },
            detection_id: { type: 'string', format: 'uuid' },
            message:      { type: 'string', example: 'Unauthorized usage detected with 94.5% similarity' },
            severity:     { type: 'string', enum: ['low', 'medium', 'high'], example: 'medium' },
            created_at:   { type: 'string', format: 'date-time' },
            detections:   { $ref: '#/components/schemas/Detection' },
          },
        },
        AlertInput: {
          type: 'object',
          required: ['detection_id', 'message', 'severity'],
          properties: {
            detection_id: { type: 'string', format: 'uuid', example: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' },
            message:      { type: 'string', example: 'Suspected counterfeit merchandise advertisement' },
            severity:     { type: 'string', enum: ['low', 'medium', 'high'], example: 'high' },
          },
        },
        Source: {
          type: 'object',
          properties: {
            id:            { type: 'string', format: 'uuid' },
            platform_name: { type: 'string', example: 'YouTube' },
            url:           { type: 'string', format: 'uri', example: 'https://youtube.com' },
            created_at:    { type: 'string', format: 'date-time' },
          },
        },
        SourceInput: {
          type: 'object',
          required: ['platform_name', 'url'],
          properties: {
            platform_name: { type: 'string', example: 'YouTube' },
            url:           { type: 'string', format: 'uri', example: 'https://youtube.com' },
          },
        },
        ErrorResponse: {
          type: 'object',
          properties: {
            success: { type: 'boolean', example: false },
            error:   { type: 'string', example: 'Descriptive error message' },
          },
        },
        ValidationError: {
          type: 'object',
          properties: {
            success: { type: 'boolean', example: false },
            error:   { type: 'string', example: 'Validation failed' },
            details: { type: 'array', items: { type: 'string' }, example: ['name is required', 'type must be "image" or "video"'] },
          },
        },
      },
    },
  },
  // No JSDoc scanning — all paths are defined inline above
  apis: [],
};

const swaggerSpec = swaggerJsdoc(options);

module.exports = swaggerSpec;
