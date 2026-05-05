const sourceService = require('../services/sourceService');
const { validate, sourceSchema } = require('../utils/validators');

/**
 * Source Controller — manage trusted platform sources.
 */
const sourceController = {
  /**
   * POST /sources
   * Body: { platform_name, url }
   */
  async create(req, res, next) {
    try {
      const result = validate(sourceSchema, req.body);

      if (!result.success) {
        return res.status(400).json({
          success: false,
          error: 'Validation failed',
          details: result.errors,
        });
      }

      const source = await sourceService.create(result.data);

      return res.status(201).json({ success: true, data: source });
    } catch (err) {
      next(err);
    }
  },

  /**
   * GET /sources
   */
  async getAll(req, res, next) {
    try {
      const sources = await sourceService.getAll();
      return res.json({ success: true, data: sources });
    } catch (err) {
      next(err);
    }
  },
};

module.exports = sourceController;
