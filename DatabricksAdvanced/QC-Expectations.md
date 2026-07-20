# Expectations

The Limits of Basic Expectations: 

A NOT NULL check confirms that a field is present — but presence alone does not mean a value is correct. 
The table below shows the range of quality problems that appear in real production data, and whether a basic NOT NULL check catches them.

| Problem Type	           | Example	                                              | NOT NULL catches it?	 | Advanced Expectation? |
|-------------------------|-------------------------------------------------------|-----------------------|-----------------------|
| Numeric anomaly	        | Negative quantity in an order	                        | ❌	                    | ✅                     |
| Temporal inconsistency	 | Event date set to year 1970 (system default)	         | ❌	                    | ✅                     |
| Range violation	        | Discount rate = 120%	                                 | ❌	                    | ✅                     |
| Optional field rule	    | Field may be NULL, but when present must be >= 0	     | ❌	                    | ✅                     |
| Schema evolution	       | New column added mid-stream breaks existing rules	    | ❌	                    | ✅                     |
| Data loss	              | Invalid records permanently dropped — no audit trail	 | ❌	                    | ✅                     |

<hr/>

##  Advanced Expectation Patterns

- [Documentation link](https://docs.databricks.com/aws/en/ldp/expectation-patterns?language=SQL)

##### 1. Row Count Validation

- Validates that row counts match between two tables — useful after joins, aggregations, or pipeline fan-out to ensure no records were silently dropped.

```sql
CREATE OR REFRESH MATERIALIZED VIEW count_verification (
  CONSTRAINT no_rows_dropped EXPECT (a_count == b_count)
    ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM
  (SELECT COUNT(*) AS a_count FROM table_a),
  (SELECT COUNT(*) AS b_count FROM table_b)
```

##### 2. Missing Record Detection

- Uses a LEFT OUTER JOIN to identify records present in a validation copy but absent in the report table — catching completeness failures that row-level checks miss entirely.

```sql
CREATE OR REFRESH MATERIALIZED VIEW report_compare_tests (
  CONSTRAINT no_missing_records EXPECT (r_key IS NOT NULL)
    ON VIOLATION FAIL UPDATE
)
AS SELECT v.*, r.key AS r_key
FROM validation_copy v
LEFT OUTER JOIN report r ON v.key = r.key
```

##### 3. Primary Key Uniqueness

- Groups by the primary key and checks that every group has exactly one entry. Catches duplicate key violations before they corrupt downstream joins or aggregations.

```sql
CREATE OR REFRESH MATERIALIZED VIEW report_pk_tests (
  CONSTRAINT unique_pk EXPECT (num_entries = 1)
    ON VIOLATION FAIL UPDATE
)
AS SELECT pk, COUNT(*) AS num_entries
FROM report
GROUP BY pk
```


## Resilient Pipeline Design

>  The most resilient bronze layer design never rejects a record due to type mismatch.
> By storing all incoming fields as STRING, - you accept whatever the source sends — integers, decimals,
> mixed types — and defer type enforcement to silver where TRY_CAST handles failures gracefully.

#### Bronze — Accept Everything

- All fields inferred as STRING. Type mismatches never fail the pipeline — an integer in a string field is just a string.
- 
```sql
CREATE OR REFRESH STREAMING TABLE bronze_events
COMMENT "Bronze: all fields as STRING, schema rescue enabled"
AS SELECT *
FROM STREAM read_files(
  '/path/to/source',
  format => 'json',
  schemaEvolutionMode => 'rescue'
)
```

#### Silver — Enforce Types Safely

> TRY_CAST returns NULL on cast failure instead of halting the pipeline. The NULL-tolerant constraint pattern then handles the rest.

```sql
CREATE OR REFRESH STREAMING TABLE silver_events (
  CONSTRAINT valid_amount EXPECT (
    CASE WHEN amount IS NOT NULL
    THEN amount >= 0 ELSE TRUE END
  ) ON VIOLATION DROP ROW
)
AS SELECT *,
  TRY_CAST(amount_str AS DOUBLE) AS amount
FROM STREAM bronze_events
```


## Schema Evolution Tools at the Bronze Layer

Two built-in mechanisms cover the full lifecycle of schema change — schemaHints for columns you know are coming, and _rescued_data as the last line of defense for anything unexpected.

#### schemaHints — Declare Future Columns Today

- Declare columns expected in upcoming files before they arrive. When the new column appears, it populates automatically. Records before the evolution carry NULL — backward and forward compatible simultaneously.

```sql

CREATE OR REFRESH STREAMING TABLE bronze_events
AS SELECT *
FROM STREAM read_files(
  '/path/to/source',
  format => 'json',
  schemaHints => 'loyalty_tier STRING, region_code STRING',
)
-- Old records: loyalty_tier = NULL (acceptable)
-- New records: loyalty_tier populated automatically
```
#### _rescued_data — Last Line of Defence

- Any field arriving outside the declared schema — unexpected columns, type mismatches — is captured as JSON in _rescued_data.
- Nothing is silently discarded. Query it at any time for investigation or recovery.

```sql
-- Inspect rescued fields after the fact
SELECT
  event_id,
  _rescued_data:unexpected_field  AS unexpected_field,
  _rescued_data:new_column        AS new_column
FROM bronze_events
WHERE _rescued_data IS NOT NULL
```

⚠️ **Critical interaction**: When a column is added via schema evolution, all records ingested before the evolution carry NULL for that column. 
Any constraint written for that column must use the NULL-tolerant CASE WHEN pattern — otherwise every historic record fails the constraint,
causing widespread false violations in the pipeline UI.

## The Quarantine Pattern

> The quarantine pattern routes every incoming record through quality evaluation, then splits output into two paths based on results—clean path for analytics and quarantine path for remediation.
> Key Guarantee: No record is ever dropped
> Mathematical Relationship: Total Records In = Clean Records + Quarantine Records

#### Zero Data Loss with Inverse Logic

**Step 1 — Quarantine Table with Inverse Logic**

- All records are written using WARN — no records are dropped. 
- The is_quarantined flag is derived using NOT(all rules): if any rule fails, the record is flagged.
- WARN is used solely to surface per-constraint metrics in the pipeline UI.

```sql
CREATE OR REFRESH STREAMING TABLE trips_quarantine (
  CONSTRAINT valid_distance EXPECT (trip_distance > 0),
  CONSTRAINT valid_fare     EXPECT (fare_amount >= 0),
  CONSTRAINT valid_pax      EXPECT (passenger_count BETWEEN 1 AND 9)
  -- WARN: surfaces metrics in UI, no records dropped
)
PARTITIONED BY (is_quarantined)
AS SELECT *,
  NOT(
    trip_distance > 0
    AND fare_amount >= 0
    AND passenger_count BETWEEN 1 AND 9
  ) AS is_quarantined
FROM STREAM bronze_trips
```

**Step 2 — Split into Clean and Failed Views**

- Two Materialized Views filter on the is_quarantined partition.
- Because the quarantine table is partitioned by is_quarantined, each view benefits from full partition pruning — only its partition is scanned, not the whole table.

```sql
-- Clean records for downstream analytics
CREATE OR REFRESH MATERIALIZED VIEW valid_trips_data
AS SELECT * FROM trips_quarantine
WHERE is_quarantined = FALSE;
-- Failed records preserved for remediation and audit
CREATE OR REFRESH MATERIALIZED VIEW invalid_trips_data
AS SELECT * FROM trips_quarantine
WHERE is_quarantined = TRUE;
```

## Choosing Between DROP ROW and Quarantine

> Both strategies enforce data quality, but they differ fundamentally in what happens to invalid records.
The right choice depends on whether your business needs an audit trail, recovery capability, and whether false violations from schema evolution are a concern.

| Action               | DROP ROW                                         | Quarantine Pattern                                                |
|----------------------|--------------------------------------------------|-------------------------------------------------------------------|
| Invalid records      | Permanently deleted                              | Preserved in quarantine table                                     |
| Audit trail          | None                                             | Full — queryable                                                  |
| Data recovery        | Not possible                                     | Fix rule → re-route from quarantine                               |
| UI violation metrics | Visible in pipeline UI                           | Per-constraint metrics via WARN                                   |
| Read performance     | Full table scan on clean data                    | Partition pruning on is_quarantined                               |
| Pipeline complexity  | Low — single table                               | Moderate — temp table + 2 views                                   |
| Best for             | Non-critical streams with well-established rules | Production pipelines with compliance, audit, or remediation needs |



### Quality metrics check


```sql
WITH quality_stats AS (
    (SELECT COUNT(*) FROM bronze.sales_raw) AS total_ingested,
    (SELECT COUNT(*) FROM silver.sales_valid) AS total_valid,
    (SELECT COUNT(*) FROM silver.sales_quarantine) AS total_quarantined
)
SELECT 
    total_ingested,
    total_valid,
    total_quarantined,
    ROUND(total_valid * 100.0 / total_ingested, 2) AS quality_score_pct,
    ROUND(total_quarantined * 100.0 / total_ingested, 2) AS failure_score_pct,
    CASE 
        WHEN total_ingested = total_valid + total_quarantined
        THEN 'ZERO DATA LOSS'
        ELSE 'DAT LOSS DETECTED'
    END AS data_loss_check
FROM quality_stats;
```