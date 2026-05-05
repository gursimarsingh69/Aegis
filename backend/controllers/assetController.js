const assetService = require('../services/assetService');
const { validate, assetUploadSchema, scanSchema } = require('../utils/validators');

/**
 * Asset Controller — thin HTTP layer.
 *
 * Handles request parsing and response formatting ONLY.
 * All business logic is delegated to assetService.
 */
const assetController = {
  // ──────────────────────────────────────────────────────────────────────────
  // FLOW 1: POST /api/assets — Register a new protected asset
  // Frontend sends: multipart/form-data with file + name + type
  // Backend generates hash via AI Engine and stores in Supabase
  // ──────────────────────────────────────────────────────────────────────────
  async create(req, res, next) {
    try {
      // With multer, text fields arrive in req.body, file in req.file
      const result = validate(assetUploadSchema, req.body);

      if (!result.success) {
        return res.status(400).json({
          success: false,
          error: 'Validation failed',
          details: result.errors,
        });
      }

      // Extract the file from multer
      const file = req.file;
      if (!file) {
        return res.status(400).json({
          success: false,
          error: 'No file uploaded. Please attach an image or video file.',
        });
      }

      // Auto-detect type from MIME if not explicitly provided
      const type = result.data.type || (file.mimetype.startsWith('video/') ? 'video' : 'image');

      // Delegate to service — it handles AI call + DB insert
      const asset = await assetService.registerAsset({
        name: result.data.name,
        type,
        fileBuffer: file.buffer,
        filename: file.originalname,
      });

      return res.status(201).json({ success: true, data: asset });
    } catch (err) {
      next(err);
    }
  },

  // ──────────────────────────────────────────────────────────────────────────
  // FLOW 2: POST /api/assets/scan — Scan media from crawler pipeline
  // Crawler sends: { url, source, processed_data, metadata }
  // Backend compares via AI Engine and stores detection + alert in DB
  // ──────────────────────────────────────────────────────────────────────────
  async scan(req, res, next) {
    try {
      // Validate crawler payload
      const result = validate(scanSchema, req.body);

      if (!result.success) {
        return res.status(400).json({
          success: false,
          error: 'Validation failed',
          details: result.errors,
        });
      }

      // Delegate to service — it handles AI comparison + detection/alert DB writes
      const matchResult = await assetService.scanMedia(result.data);

      return res.json({ success: true, data: matchResult });
    } catch (err) {
      next(err);
    }
  },

  // ──────────────────────────────────────────────────────────────────────────
  // GET /api/assets — List all registered assets
  // ──────────────────────────────────────────────────────────────────────────
  async getAll(req, res, next) {
    try {
      const assets = await assetService.getAll();
      return res.json({ success: true, data: assets });
    } catch (err) {
      next(err);
    }
  },

  // ──────────────────────────────────────────────────────────────────────────
  // GET /api/assets/:id — Get a single asset by ID
  // ──────────────────────────────────────────────────────────────────────────
  async getById(req, res, next) {
    try {
      const asset = await assetService.getById(req.params.id);

      if (!asset) {
        return res.status(404).json({
          success: false,
          error: 'Asset not found',
        });
      }

      return res.json({ success: true, data: asset });
    } catch (err) {
      next(err);
    }
  },
};

module.exports = assetController;
