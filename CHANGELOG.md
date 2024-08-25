# CHANGELOG
## [2.1.0] dbt-dagster-duckdb with S3 lake (WIP)
- Refactor dagster to individual code spaces.
- Breakdown operations into ops and only declare assets as materialized items.
- Integrate S3 data store.
- Integrate MotherDuck for cloud data warehouse.

## [2.0.0] dbt-dagster-duckdb
- Integrate dagster (orchestration), dbt (transform), duckdb (data warehouse).
- Defined pipeline as assets and jobs.
### Added
- dagster_elt
- dbt_elt

## [1.0.0] Initial project
- Integrate dbt with mysql for HDB resale price pipeline.
- API call and manual sql ingestion to mysql.