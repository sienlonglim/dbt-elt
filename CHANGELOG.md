# CHANGELOG
## Plans
- Explore MotherDuck for cloud data warehouse.
- Explore Terraform.
- Write unit tests.
- Refactor weather data code.
- Build out DWH to mart
- Build backend API

## [2.1.0] dbt-dagster-duckdb with S3 lake (WIP)
- Refactored dagster to individual code spaces.
- Refactored asset for hdb data into ops and jobs.
- Integrate S3 data store.

## [2.0.0] dbt-dagster-duckdb
- Integrated dagster (orchestration), dbt (transform), local duckdb (data warehouse).
- Defined pipeline as assets and jobs.
### Added
- dagster_elt
- dbt_elt

## [1.0.0] Initial project
- Integrated dbt with mysql for HDB resale price pipeline.
- API call and manual sql ingestion to mysql.