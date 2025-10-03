CREATE TABLE IF NOT EXISTS items (
  id TEXT PRIMARY KEY,
  url TEXT,
  source TEXT,
  lang TEXT,
  text TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predictions (
  id TEXT REFERENCES items(id) ON DELETE CASCADE,
  top_label TEXT,
  confidence REAL,
  raw JSONB,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_predictions_label ON predictions(top_label);
CREATE INDEX IF NOT EXISTS idx_predictions_created ON predictions(created_at DESC);
