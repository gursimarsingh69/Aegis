const alertService = require('../services/alertService');
const { validate, alertSchema } = require('../utils/validators');

const VALID_SEVERITIES = ['low', 'medium', 'high'];

/**
 * Alert Controller — manage alerts for suspicious detections.
 */
const alertController = {
  /**
   * POST /alerts
   * Body: { detection_id, message, severity }
   */
  async create(req, res, next) {
    try {
      const result = validate(alertSchema, req.body);

      if (!result.success) {
        return res.status(400).json({
          success: false,
          error: 'Validation failed',
          details: result.errors,
        });
      }

      const alert = await alertService.create(result.data);

      return res.status(201).json({ success: true, data: alert });
    } catch (err) {
      next(err);
    }
  },

  /**
   * GET /alerts
   * Query params: severity (optional), limit (optional)
   */
  async getAll(req, res, next) {
    try {
      const { severity, limit } = req.query;

      if (severity && !VALID_SEVERITIES.includes(severity)) {
        return res.status(400).json({
          success: false,
          error: `Invalid severity filter. Must be one of: ${VALID_SEVERITIES.join(', ')}`,
        });
      }

      const alerts = await alertService.getAll({
        severity: severity || undefined,
        limit: limit ? parseInt(limit, 10) : undefined,
      });

      return res.json({ success: true, data: alerts });
    } catch (err) {
      next(err);
    }
  },

  /**
   * GET /alerts/:id
   */
  async getById(req, res, next) {
    try {
      const alert = await alertService.getById(req.params.id);

      if (!alert) {
        return res.status(404).json({
          success: false,
          error: 'Alert not found',
        });
      }

      return res.json({ success: true, data: alert });
    } catch (err) {
      next(err);
    }
  },
};

module.exports = alertController;
