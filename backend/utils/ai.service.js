const axios = require('axios');
const FormData = require('form-data');
const { aiEngineUrl } = require('../config/env');

/**
 * AI Service — HTTP bridge to the FastAPI AI Engine at :8000.
 *
 * This module has NO database access. It is a pure computation proxy.
 * All it does is forward files/data to the AI Engine and return the results.
 *
 * Endpoints called:
 *   POST {AI_ENGINE_URL}/hash    — compute perceptual hashes for an asset
 *   POST {AI_ENGINE_URL}/compare — compare suspicious media against registered hashes
 */

/**
 * Generate perceptual hash signatures for an uploaded asset.
 * Calls POST /hash on the AI Engine.
 *
 * @param {Buffer} fileBuffer — the raw file bytes
 * @param {string} filename   — original filename (for content-type detection)
 * @returns {Promise<string>}  JSON string of the hash signature
 */
async function generateHash(fileBuffer, filename) {
  const form = new FormData();
  form.append('file', fileBuffer, { filename: filename || 'upload.jpg' });

  try {
    const response = await axios.post(`${aiEngineUrl}/hash`, form, {
      headers: form.getHeaders(),
      timeout: 30000, // 30s timeout for AI processing
      maxContentLength: 50 * 1024 * 1024,
    });

    const { hash_signature } = response.data;
    return JSON.stringify(hash_signature);
  } catch (err) {
    console.error('AI Engine /hash error:', err.message);
    // Fallback: generate a placeholder hash so registration doesn't fail
    // when the AI Engine is unavailable
    const crypto = require('crypto');
    const seed = filename || `asset_${Date.now()}`;
    const base = crypto.createHash('sha256').update(seed).digest('hex');
    const fallback = {
      phash: base.slice(0, 16),
      dhash: base.slice(16, 32),
      ahash: base.slice(32, 48),
      chash: base.slice(48, 64),
      width: 0,
      height: 0,
      blur_index: 0,
      _fallback: true,
    };
    return JSON.stringify(fallback);
  }
}

/**
 * Compare suspicious media against registered asset hashes.
 * Calls POST /compare on the AI Engine.
 *
 * @param {Buffer} fileBuffer       — the suspicious file bytes
 * @param {string} filename         — original filename
 * @param {object[]} registeredAssets — assets from Supabase [{id, hash_signature, ...}]
 * @returns {Promise<{ matched: boolean, similarity_score: number, asset_id: string|null, reason: string }>}
 */
async function compareHashes(fileBuffer, filename, registeredAssets) {
  // Build the minimal asset list the AI Engine needs (id + hash_signature only)
  const assetsPayload = registeredAssets.map((a) => ({
    id: a.id,
    hash_signature: a.hash_signature,
  }));

  const form = new FormData();
  form.append('file', fileBuffer, { filename: filename || 'suspicious.jpg' });
  form.append('assets', JSON.stringify(assetsPayload));

  try {
    const response = await axios.post(`${aiEngineUrl}/compare`, form, {
      headers: form.getHeaders(),
      timeout: 30000,
      maxContentLength: 50 * 1024 * 1024,
    });

    const result = response.data;
    return {
      matched: result.match || false,
      similarity_score: result.similarity_score || 0,
      asset_id: result.matched_asset_id || null,
      reason: result.reason || 'Analyzed via AI Engine',
    };
  } catch (err) {
    console.error('AI Engine /compare error:', err.message);
    return {
      matched: false,
      similarity_score: 0,
      asset_id: null,
      reason: `AI Engine unavailable: ${err.message}`,
    };
  }
}

module.exports = { generateHash, compareHashes };
