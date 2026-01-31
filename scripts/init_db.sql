CREATE TABLE IF NOT EXISTS run_audit (
  run_id TEXT PRIMARY KEY,
  dag_id TEXT NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  status TEXT NOT NULL,
  raw_path TEXT,
  curated_path TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS daily_metrics (
  dt DATE NOT NULL,
  event_type TEXT NOT NULL,
  cnt BIGINT NOT NULL,
  PRIMARY KEY (dt, event_type)
);

CREATE TABLE IF NOT EXISTS quality_results (
  run_id TEXT NOT NULL,
  check_name TEXT NOT NULL,
  passed BOOLEAN NOT NULL,
  details TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_files (
  raw_file TEXT PRIMARY KEY,
  dt DATE NOT NULL,
  run_id TEXT NOT NULL,
  processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS run_stats (
  run_id TEXT PRIMARY KEY,
  dt DATE NOT NULL,
  raw_file TEXT NOT NULL,
  input_rows BIGINT NOT NULL,
  curated_rows BIGINT NOT NULL,
  metrics_rows BIGINT NOT NULL,
  curated_bytes BIGINT NOT NULL,
  metrics_bytes BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
