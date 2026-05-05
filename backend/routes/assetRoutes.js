const express = require('express');
const router = express.Router();
const assetController = require('../controllers/assetController');
const upload = require('../middleware/upload');

// FLOW 1: Register a new protected asset (Frontend → Backend → AI → DB)
// Accepts multipart/form-data with file + name + type
router.post('/', upload.single('file'), assetController.create);

// FLOW 2a: Scan scraped media via JSON payload (legacy)
router.post('/scan', assetController.scan);

// FLOW 2b: Scan scraped media via raw file upload (Crawler → Backend → AI → DB)
// Crawler sends multipart/form-data with: file + url + source
router.post('/scan/file', upload.single('file'), assetController.scanFile);

// Read operations
router.get('/', assetController.getAll);
router.get('/:id', assetController.getById);

module.exports = router;
