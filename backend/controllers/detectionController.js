const detectionService = require('../services/detectionService');
const { validate, detectionSchema } = require('../utils/validators');

/**
 * Detection Controller — ingest detected media usage from crawlers/AI pipeline.
 */
const detectionController = {
  /**
   * POST /detections
   * Body: { asset_id, source_url, similarity_score }
   *
   * This endpoint simulates the ingestion point for a crawler or AI pipeline.
   * It automatically:
   *   1. Determines if the source is authorized (trusted platform)
   *   2. Creates an alert if similarity is high and source is unauthorized
   */
  async create(req, res, next) {
    try {
      const result = validate(detectionSchema, req.body);

      if (!result.success) {
        return res.status(400).json({
          success: false,
          error: 'Validation failed',
          details: result.errors,
        });
      }

      const { detection, alert } = await detectionService.create(result.data);

      return res.status(201).json({
        success: true,
        data: {
          detection,
          alert_generated: alert !== null,
          alert,
        },
      });
    } catch (err) {
      next(err);
    }
  },

  /**
   * GET /detections
   * Query params: minScore, isAuthorized, limit
   */
  async getAll(req, res, next) {
    try {
      const { minScore, isAuthorized, limit } = req.query;

      const detections = await detectionService.getAll({
        minScore: minScore ? parseFloat(minScore) : undefined,
        isAuthorized: isAuthorized !== undefined ? isAuthorized === 'true' : undefined,
        limit: limit ? parseInt(limit, 10) : undefined,
      });

      return res.json({ success: true, data: detections });
    } catch (err) {
      next(err);
    }
  },

  /**
   * GET /detections/:id
   */
  async getById(req, res, next) {
    try {
      const detection = await detectionService.getById(req.params.id);

      if (!detection) {
        return res.status(404).json({
          success: false,
          error: 'Detection not found',
        });
      }

      return res.json({ success: true, data: detection });
    } catch (err) {
      next(err);
    }
  },
};

module.exports = detectionController;
