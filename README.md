# dbt_athena_poc

dbt project using the **dbt Athena adapter** against AWS Data staging (Glue + S3 Iceberg).

## Overview

This project builds a **denormalized segment-card** dataset from three existing Iceberg “sourcing” tables in Athena/Glue.

- **Staging**: views on sources (`stg_*`)
- **Intermediate**: physical **incremental Iceberg table** (`int_segment_card`)
- **Marts**: views/tables for consumption (`mart_*`)

## Source tables (Glue / Athena)

Sources (Glue DB): `staging_factorypal_datalake_sourcing`

- `segments__public__segments`
- `segments__public__cards`
- `segments__public__card_segment`

## Prerequisite: configure AWS SSO profile (AWS CLI)

This project assumes you have an AWS CLI **named profile** called `data` (or adjust the name in `profiles.yml`).

### Login (repeat whenever the SSO session expires):

```bash
aws sso login --profile data
```

## Local setup

### 1) Install dbt + Athena adapter

```bash
python -m pip install dbt-core dbt-athena-community
dbt --version
```

### 2) Get the project

Option A (recommended): **use the project already in this repo**

```bash
cd dbt_athena_poc
```

Option B: **initialize a new project** (if starting from scratch)

```bash
dbt init dbt_athena_poc
```

### 3) AWS login (SSO)

```bash
aws sso login --profile data
```

### 4) dbt profile configuration

This project uses a **project-local** dbt profile at `dbt_athena_poc/profiles.yml`.

Key settings:
- `database: awsdatacatalog` (Glue catalog)
- `schema: staging_factorypal_datalake_sourcing` (Glue DB where dbt creates relations)
- `s3_staging_dir` (Athena query results output location)
- `s3_data_dir` (Iceberg table storage)

### 5) Run commands (debug → compile → build)

From `dbt_athena_poc/`:

```bash
dbt debug --profiles-dir . --target data
dbt compile --profiles-dir . --target data
dbt build --profiles-dir . --target data
```

### 7) Common run patterns

Build only the denormalized outputs:

```bash
dbt run --profiles-dir . --target data --select int_segment_card mart_denorm_segment_card
```

Limit rows for development/testing:

```bash
dbt run --profiles-dir . --target data --select int_segment_card --vars '{row_limit: 200}'
```

Tune incremental merge scan window:

```bash
dbt run --profiles-dir . --target data --select int_segment_card --vars '{incremental_merge_lookback_days: 14}'
```

## dbt docs

```bash
dbt docs generate --profiles-dir . --target data
dbt docs serve --profiles-dir . --target data --host 0.0.0.0 --port 8080
```
Open `http://localhost:8080`.