![CI](https://github.com/tharun7183/data-platform-guardrails/actions/workflows/ci.yml/badge.svg)

# Spark + Airflow Data Platform Guardrails (Mini Project)

A GitHub-ready mini-project that mirrors real Data Platform work:
- **Airflow** orchestrates the full lifecycle
- **Spark** transforms high-volume analytics events into curated datasets + warehouse metrics
- **Guardrails**: data quality checks + drop-rate threshold
- **Automation**: incremental processing manifest prevents reprocessing the same raw file
- **Cost efficiency**: run-level stats track row counts + Parquet byte sizes
- **Operational visibility**: run audits and retries

## Architecture

Raw Events (JSONL) ➜ Spark (dedupe + curate + metrics) ➜ Parquet outputs  
                                           ↘ quality_results (Postgres)  
                                           ↘ run_stats (bytes/rows) (Postgres)  
                                           ↘ processed_files (manifest) (Postgres)  

Airflow orchestrates end-to-end with retries and **short-circuits** if the raw file was already processed.

## Quickstart

### Prereqs
- Docker + Docker Compose

### Start stack
```bash
make up
make init
```

### UIs
- Airflow: http://localhost:8088 (admin / admin)
- Spark: http://localhost:8080

### Run the pipeline
```bash
make run
```

## Inspect Results

```bash
make psql
```

Then:
```sql
select * from run_audit order by started_at desc limit 5;
select * from quality_results order by created_at desc limit 20;
select * from run_stats order by created_at desc limit 5;
select * from processed_files order by processed_at desc limit 5;
```

## Why this maps to Data Platform roles

- **Reliability**: idempotency via dedupe + manifest
- **Guardrails**: automated quality checks
- **Cost efficiency**: output size and row-count stats
- **Self-serve**: one-command start + run
- **Operational excellence**: audit trail, retries, skip logic
