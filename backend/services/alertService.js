const supabase = require('../config/supabase');

/**
 * Alert Service — handles all alert-related database operations.
 */
const alertService = {
  /**
   * Create a new alert (manual).
   * @param {{ detection_id: string, message: string, severity: string }} data
   * @returns {Promise<object>} The created alert row (with nested detection + asset)
   */
  async create({ detection_id, message, severity }) {
    const { data, error } = await supabase
      .from('alerts')
      .insert({ detection_id, message, severity })
      .select('*, detections(*, assets(*))')
      .single();

    if (error) throw error;
    return data;
  },

  /**
   * Auto-create an alert from the detection pipeline.
   * Determines severity based on similarity score:
   *   ≥ 95 → high
   *   ≥ 90 → medium
   *   ≥ threshold → low
   * @param {{ detection_id: string, similarity_score: number }} data
   * @returns {Promise<object>} The created alert
   */
  async autoCreate({ detection_id, similarity_score }) {
    let severity = 'low';
    if (similarity_score >= 95) {
      severity = 'high';
    } else if (similarity_score >= 90) {
      severity = 'medium';
    }

    const message = `Unauthorized usage detected with ${similarity_score}% similarity`;

    const { data, error } = await supabase
      .from('alerts')
      .insert({ detection_id, message, severity })
      .select('*, detections(*, assets(*))')
      .single();

    if (error) throw error;
    return data;
  },

  /**
   * Fetch all alerts with detection + asset info.
   * Supports optional severity filter.
   * @param {{ severity?: string, limit?: number }} options
   * @returns {Promise<object[]>}
   */
  async getAll({ severity, limit } = {}) {
    let query = supabase
      .from('alerts')
      .select('*, detections(*, assets(*))')
      .order('created_at', { ascending: false });

    if (severity) {
      query = query.eq('severity', severity);
    }

    if (limit) {
      query = query.limit(limit);
    }

    const { data, error } = await query;
    if (error) throw error;
    return data;
  },

  /**
   * Fetch a single alert by ID with full details.
   * @param {string} id
   * @returns {Promise<object|null>}
   */
  async getById(id) {
    const { data, error } = await supabase
      .from('alerts')
      .select('*, detections(*, assets(*))')
      .eq('id', id)
      .single();

    if (error) throw error;
    return data;
  },
};

module.exports = alertService;
