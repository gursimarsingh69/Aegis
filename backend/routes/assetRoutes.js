const express = require('express');
const router = express.Router();
const assetController = require('../controllers/assetController');
const upload = require('../middleware/upload');

// FLOW 1: Register a new protected asset (Frontend → Backend → AI → DB)
// Accepts multipart/form-data with file + name + type
router.post('/', upload.single('file'), assetController.create);

// FLOW 2: Scan scraped media (Crawler → Backend → AI → DB)
// Accepts JSON with processed_data (base64 frames)
router.post('/scan', assetController.scan);

// Read operations
router.get('/', assetController.getAll);
router.get('/:id', assetController.getById);

module.exports = router;
