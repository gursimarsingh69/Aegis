const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');
const swaggerUi = require('swagger-ui-express');
const swaggerSpec = require('./config/swagger');

const routes = require('./routes');
const errorHandler = require('./middleware/errorHandler');
const { port, nodeEnv } = require('./config/env');

const app = express();

// ---------------------------------------------------------------------------
// Security & Utility Middleware
// ---------------------------------------------------------------------------
app.use(helmet());                         // Security headers
app.use(cors());                           // CORS (configure origins for prod)
app.use(express.json({ limit: '50mb' }));   // JSON body parser (large for crawler base64 frames)
app.use(morgan('dev'));                     // Request logging

// Rate limiting — 100 requests per 15 minutes per IP
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
  message: { success: false, error: 'Too many requests, please try again later.' },
});
app.use('/api', limiter);

// ---------------------------------------------------------------------------
// Health Check
// ---------------------------------------------------------------------------
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), timestamp: new Date().toISOString() });
});

// ---------------------------------------------------------------------------
// Swagger API Documentation
// ---------------------------------------------------------------------------
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec, {
  customCss: '.swagger-ui .topbar { display: none }',
  customSiteTitle: 'Sports Media Detection API Docs',
}));

// Serve raw OpenAPI JSON spec
app.get('/api-docs.json', (_req, res) => {
  res.json(swaggerSpec);
});

// ---------------------------------------------------------------------------
// API Routes — mounted at /api
// ---------------------------------------------------------------------------
app.use('/api', routes);

// ---------------------------------------------------------------------------
// 404 Handler
// ---------------------------------------------------------------------------
app.use((_req, res) => {
  res.status(404).json({ success: false, error: 'Route not found' });
});

// ---------------------------------------------------------------------------
// Global Error Handler
// ---------------------------------------------------------------------------
app.use(errorHandler);

// ---------------------------------------------------------------------------
// Start Server
// ---------------------------------------------------------------------------
app.listen(port, () => {
  console.log(`\n🚀  Sports Media Detection API running in ${nodeEnv} mode on http://localhost:${port}`);
  console.log(`📡  API base:  http://localhost:${port}/api`);
  console.log(`📖  Docs:     http://localhost:${port}/api-docs`);
  console.log(`💚  Health:    http://localhost:${port}/health\n`);
});

module.exports = app;
