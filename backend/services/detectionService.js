const supabase = require('../config/supabase');
const sourceService = require('./sourceService');
const alertService = require('./alertService');
const { similarityThreshold } = require('../config/env');

/**
 * Detection Service — handles detection ingestion and near-real-time processing.
 *
 * When a detection is created:
 *   1. Check if the source URL is from a trusted platform → set is_authorized
 *   2. Insert the detection record
 *   3. If similarity_score ≥ threshold AND unauthorized → auto-create alert
 */
const detectionService = {
  /**
   * Create a new detection with auto-authorization and alert logic.
   * @param {{ asset_id: string, source_url: string, similarity_score: number }} data
   * @returns {Promise<{ detection: object, alert: object|null }>}
   */
  async create({ asset_id, source_url, similarity_score }) {
    // Step 1: Auto-determine authorization
    const is_authorized = await sourceService.isTrusted(source_url);

    // Step 2: Insert detection
    const { data: detection, error } = await supabase
      .from('detections')
      .insert({ asset_id, source_url, similarity_score, is_authorized })
      .select('*, assets(*)')
      .single();

    if (error) throw error;

    // Step 3: Auto-generate alert if unauthorized and above threshold
    let alert = null;
    if (!is_authorized && similarity_score >= similarityThreshold) {
      alert = await alertService.autoCreate({
        detection_id: detection.id,
        similarity_score,
      });
    }

    return { detection, alert };
  },

  /**
   * Fetch all detections with their related asset details.
   * Supports optional query filters.
   * @param {{ minScore?: number, isAuthorized?: boolean, limit?: number }} options
   * @returns {Promise<object[]>}
   */
  async getAll({ minScore, isAuthorized, limit } = {}) {
    let query = supabase
      .from('detections')
      .select('*, assets(*)')
      .order('detected_at', { ascending: false });

    if (minScore !== undefined) {
      query = query.gte('similarity_score', minScore);
    }

    if (isAuthorized !== undefined) {
      query = query.eq('is_authorized', isAuthorized);
    }

    if (limit) {
      query = query.limit(limit);
    }

    const { data, error } = await query;
    if (error) throw error;
    return data;
  },

  /**
   * Fetch a single detection by ID with asset details.
   * @param {string} id
   * @returns {Promise<object|null>}
   */
  async getById(id) {
    const { data, error } = await supabase
      .from('detections')
      .select('*, assets(*)')
      .eq('id', id)
      .single();

    if (error) throw error;
    return data;
  },
};

module.exports = detectionService;
