CREATE TABLE IF NOT EXISTS consistency (
  id TEXT PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE,
  img_text_sim REAL,
  ocr_chars INTEGER,
  ocr_preview TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_consistency_created ON consistency(created_at DESC);
