const { z } = require('zod');

// ── Asset Upload Validation ──────────────────────────────────────────────────
// Frontend sends file via multipart + name + optional type.
// Backend auto-detects type from MIME if not provided.
const assetSchema = z.object({
  name: z
    .string()
    .max(200, 'name must be 200 characters or fewer')
    .optional(), // auto-filled from filename if not provided
  type: z.enum(['image', 'video'], {
    invalid_type_error: 'type must be "image" or "video"',
  }).optional(),
});

// Alias for clarity in controller imports
const assetUploadSchema = assetSchema;

// ── Scan Validation (Crawler Pipeline payload) ──────────────────────────────
// The crawler sends preprocessed media data for comparison against registered assets.
const scanSchema = z.object({
  url: z
    .string({ required_error: 'url is required' })
    .min(1, 'url must not be empty'),
  source: z
    .string()
    .min(1)
    .default('unknown'),
  timestamp: z
    .string()
    .optional(),
  media_type: z.enum(['image', 'video'], {
    invalid_type_error: 'media_type must be "image" or "video"',
  }).default('image'),
  processed_data: z
    .array(z.string())
    .min(1, 'processed_data must contain at least one frame'),
  metadata: z.object({
    post_url: z.string().optional(),
    content_type: z.string().optional(),
    keyword_matched: z.string().optional(),
  }).optional(),
});

// ── Detection Validation ────────────────────────────────────────────────────
const detectionSchema = z.object({
  asset_id: z
    .string({ required_error: 'asset_id is required' })
    .uuid('asset_id must be a valid UUID'),
  source_url: z
    .string({ required_error: 'source_url is required' })
    .url('source_url must be a valid URL'),
  similarity_score: z
    .number({ required_error: 'similarity_score is required', invalid_type_error: 'similarity_score must be a number' })
    .min(0, 'similarity_score must be at least 0')
    .max(100, 'similarity_score must be at most 100'),
});

// ── Alert Validation ────────────────────────────────────────────────────────
const alertSchema = z.object({
  detection_id: z
    .string({ required_error: 'detection_id is required' })
    .uuid('detection_id must be a valid UUID'),
  message: z
    .string({ required_error: 'message is required' })
    .min(1, 'message must not be empty'),
  severity: z.enum(['low', 'medium', 'high'], {
    required_error: 'severity is required',
    invalid_type_error: 'severity must be "low", "medium", or "high"',
  }),
});

// ── Source Validation ───────────────────────────────────────────────────────
const sourceSchema = z.object({
  platform_name: z
    .string({ required_error: 'platform_name is required' })
    .min(1, 'platform_name must not be empty')
    .max(100, 'platform_name must be 100 characters or fewer'),
  url: z
    .string({ required_error: 'url is required' })
    .url('url must be a valid URL'),
});

/**
 * Validate data against a Zod schema.
 * Returns { success: true, data } or { success: false, errors: string[] }.
 */
function validate(schema, data) {
  const result = schema.safeParse(data);

  if (result.success) {
    return { success: true, data: result.data };
  }

  const errors = result.error.issues.map((issue) => issue.message);
  return { success: false, errors };
}

module.exports = {
  assetSchema,
  assetUploadSchema,
  scanSchema,
  detectionSchema,
  alertSchema,
  sourceSchema,
  validate,
};
