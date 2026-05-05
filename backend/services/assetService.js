const supabase = require('../config/supabase');
const { generateHash, compareHashes } = require('../utils/ai.service');
const { similarityThreshold } = require('../config/env');

/**
 * Asset Service — orchestrates both upload and scan workflows.
 *
 * FLOW 1 (registerAsset):  Frontend → validate → AI hash → store in Supabase
 * FLOW 2 (scanMedia):      Crawler  → validate → fetch assets → AI compare → store detection/alert
 *
 * All business logic lives here. Controller is thin (HTTP only).
 * AI service is stateless (no DB access).
 */
const assetService = {
  // ──────────────────────────────────────────────────────────────────────────
  // FLOW 1: Register a new protected asset
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Register a new asset:
   *   1. Call AI engine to generate perceptual hashes from the actual file
   *   2. Store the asset + hash_signature in Supabase
   *   3. Return the created asset
   *
   * @param {{ name: string, type: string, fileBuffer: Buffer, filename: string }} params
   * @returns {Promise<object>} The created asset row
   */
  async registerAsset({ name, type, fileBuffer, filename }) {
    // Step 1: Call AI engine to generate hash signature from actual file
    const hash_signature = await generateHash(fileBuffer, filename);

    // Step 2: Store in Supabase — backend controls the DB write
    const { data, error } = await supabase
      .from('assets')
      .insert({
        name,
        type,
        hash_signature,
        owner: 'System',
      })
      .select()
      .single();

    if (error) throw error;

    // Step 3: Return the created asset
    return data;
  },

  // ──────────────────────────────────────────────────────────────────────────
  // FLOW 2: Scan scraped media against registered assets
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Scan media from the crawler pipeline:
   *   1. Fetch all registered assets from Supabase
   *   2. Decode the first base64 frame to a buffer
   *   3. Call AI engine to compare against registered hashes
   *   4. If match found, create detection record
   *   5. If score >= threshold, auto-generate alert
   *   6. Return match result
   *
   * @param {{ url: string, source: string, media_type: string, processed_data: string[], metadata?: object }} params
   * @returns {Promise<object>} Match result with similarity score
   */
  async scanMedia({ url, source, media_type, processed_data, metadata }) {
    // Step 1: Fetch all registered assets with their hash signatures
    const { data: assets, error: fetchError } = await supabase
      .from('assets')
      .select('*');

    if (fetchError) throw fetchError;

    // Step 2: Decode the first base64 frame to a file buffer
    const firstFrame = processed_data[0];
    const fileBuffer = Buffer.from(firstFrame, 'base64');
    const filename = `scan_${Date.now()}.jpg`;

    // Step 3: Call AI engine to compare — NO DB access in AI
    const result = await compareHashes(fileBuffer, filename, assets);

    // Step 4: If match found, create a detection record
    if (result.matched && result.asset_id) {
      const sourceUrl = (metadata && metadata.post_url) || url;

      const { error: detectionError } = await supabase
        .from('detections')
        .insert({
          asset_id: result.asset_id,
          source_url: sourceUrl,
          similarity_score: result.similarity_score,
          is_authorized: false,
        });

      if (detectionError) {
        console.error('Failed to insert detection:', detectionError.message);
        // Don't throw — still return the match result
      }

      // Step 5: Auto-generate alert if score exceeds threshold
      if (result.similarity_score >= similarityThreshold) {
        const severity =
          result.similarity_score >= 90 ? 'high' :
          result.similarity_score >= 70 ? 'medium' : 'low';

        // First get the detection ID for the alert
        const { data: detection } = await supabase
          .from('detections')
          .select('id')
          .eq('asset_id', result.asset_id)
          .eq('source_url', sourceUrl)
          .order('detected_at', { ascending: false })
          .limit(1)
          .single();

        if (detection) {
          const { error: alertError } = await supabase
            .from('alerts')
            .insert({
              detection_id: detection.id,
              message: `Unauthorized usage detected from ${source} — similarity ${result.similarity_score}%`,
              severity,
            });

          if (alertError) {
            console.error('Failed to insert alert:', alertError.message);
          }
        }
      }
    }

    // Step 6: Return result to caller (frontend or crawler)
    return {
      matched: result.matched,
      is_match: result.matched,
      similarity_score: result.similarity_score,
      score: result.similarity_score,
      matched_asset: result.asset_id,
      reason: result.reason,
    };
  },

  // ──────────────────────────────────────────────────────────────────────────
  // READ operations (unchanged)
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Fetch all assets, newest first.
   * @returns {Promise<object[]>}
   */
  async getAll() {
    const { data, error } = await supabase
      .from('assets')
      .select('*')
      .order('created_at', { ascending: false });

    if (error) throw error;
    return data;
  },

  /**
   * Fetch a single asset by ID.
   * @param {string} id
   * @returns {Promise<object|null>}
   */
  async getById(id) {
    const { data, error } = await supabase
      .from('assets')
      .select('*')
      .eq('id', id)
      .single();

    if (error) throw error;
    return data;
  },
};

module.exports = assetService;
