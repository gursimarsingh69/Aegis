-- ============================================================
-- Sports Media Detection System — Supabase/PostgreSQL Schema
-- Run this in the Supabase SQL Editor (Dashboard → SQL Editor)
-- ============================================================

-- Drop existing tables if migrating from old schema
DROP TABLE IF EXISTS alerts   CASCADE;
DROP TABLE IF EXISTS matches  CASCADE;
DROP TABLE IF EXISTS detections CASCADE;
DROP TABLE IF EXISTS assets   CASCADE;
DROP TABLE IF EXISTS sources  CASCADE;

-- ─────────────────────────────────────────────────────────────
-- 1. Sources — trusted platforms / authorized distributors
-- ─────────────────────────────────────────────────────────────
CREATE TABLE sources (
  id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  platform_name TEXT        NOT NULL,
  url           TEXT        NOT NULL UNIQUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────────────────────
-- 2. Assets — registered official media
-- ─────────────────────────────────────────────────────────────
CREATE TABLE assets (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  name            TEXT        NOT NULL,
  type            TEXT        NOT NULL CHECK (type IN ('image', 'video')),
  hash_signature  TEXT        NOT NULL,
  owner           TEXT        NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────────────────────
-- 3. Detections — where/how assets are found online
-- ─────────────────────────────────────────────────────────────
CREATE TABLE detections (
  id               UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  asset_id         UUID         NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
  source_url       TEXT         NOT NULL,
  detected_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
  similarity_score NUMERIC(5,2) NOT NULL CHECK (similarity_score >= 0 AND similarity_score <= 100),
  is_authorized    BOOLEAN      NOT NULL DEFAULT false
);

-- ─────────────────────────────────────────────────────────────
-- 4. Alerts — notifications for suspicious detections
-- ─────────────────────────────────────────────────────────────
CREATE TABLE alerts (
  id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  detection_id  UUID        NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
  message       TEXT        NOT NULL,
  severity      TEXT        NOT NULL CHECK (severity IN ('low', 'medium', 'high')),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────────────────────
-- Indexes for common query patterns
-- ─────────────────────────────────────────────────────────────
CREATE INDEX idx_detections_asset_id    ON detections(asset_id);
CREATE INDEX idx_detections_detected_at ON detections(detected_at DESC);
CREATE INDEX idx_detections_authorized  ON detections(is_authorized);
CREATE INDEX idx_alerts_detection_id    ON alerts(detection_id);
CREATE INDEX idx_alerts_severity        ON alerts(severity);
CREATE INDEX idx_alerts_created_at      ON alerts(created_at DESC);
CREATE INDEX idx_sources_url            ON sources(url);

-- ─────────────────────────────────────────────────────────────
-- Row Level Security (enable once you add policies)
-- ─────────────────────────────────────────────────────────────
-- ALTER TABLE sources    ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE assets     ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE detections ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE alerts     ENABLE ROW LEVEL SECURITY;
