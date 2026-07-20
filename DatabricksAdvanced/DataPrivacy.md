## Regulatory Compliance

- GDPR (General Data Protection Regulation) -> EU 
- CCPA (California Consumer Privacy Act) -> USA
- HIPAA (Health Insurance Portability and Accountability Act) -> USA
- Simplified Compliance Requirements

> Audits that find companies out of compliance can be extremely costly
to the company, and scaling that for every user adds even more expenses. Under the
GDPR, if companies don’t respond to requests **within 30 days**, they can be penalized
up to 4% of their annual revenue or 20 million euros, whichever is greater. Under the
CCPA, some additional stuff you have to confirm receipt **within 10 business days** of
requests process **within 45 days**, and finds up to 2,500 per violation and 750 per
consumer per incident.

<hr/>

### Databricks Regulatory Compliance

+ Reduce copies of your PII
+ Find personal information quickly
+ Reliably change, delete, or export data
+ Built-in data skipping optimizations (Z-order) and housekeeping of
obsolete/deleted data (VACUUM)
+ Use transaction logs for auditing

<hr/>

## Data Privacy - 3 Key Aspects

1. Identify
2. Protect ( anonymize/ pseudonymize/pass )
3. Manage

<hr/>

## Fine-grained Access Control

1. Row based limit

```sql
-- function for masking
CREATE FUNCTION us_filter(region STRING)
RETURN IF(IS_MEMBER(‘admin’), true, region=“US”);
-- modify table for using mask on row
ALTER TABLE sales SET ROW FILTER us_filter ON region;

```
2. Column based limit

```sql
CREATE FUNCTION ssn_mask(ssn STRING)
    RETURN CASE WHEN IS_MEMBER(‘admin’) THEN ssn
ELSE '*****'
END;
-- modify table for using mask on column
ALTER TABLE users ALTER COLUMN table_ssn SET MASK ssn_mask;
```
- Data Masking

<hr/>

## Data Audit

System Tables: Object Metadata

**What tables are in the sales catalog?**

```sql
SELECT table_name
FROM system.information_schema.tables
WHERE table_catalog="sales";
```


**Who last updated the gold tables and when?** 

```sql
SELECT table_name, last_altered_by, last_altered
FROM system.information_schema.tables
WHERE table_schema = "churn_gold"
ORDER BY 1, 3 DESC;
```

**Who has access to this table?**

```sql
SELECT grantee, table_name, privilege_type
FROM system.information_schema.table_privileges
WHERE table_name = "login_data_silver";
```

**Who owns this gold table?** 

```sql
SELECT table_owner
FROM system.information_schema.tables
WHERE table_catalog = "retail_prod" AND
table_schema = "churn_gold" AND table_name =
"churn_features"; 
```

### Documentation

- [System tables reference](https://docs.databricks.com/aws/en/admin/system-tables)
- [Information schema](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-information-schema)



### System Tables: Billing Logs

**What is the daily trend in DBU consumption?**

```sql
SELECT usage_date as `Date`, sum(usage_quantity) as `DBUs Consumed`
 FROM system.billing.usage
GROUP BY usage_date
ORDER BY usage_date ASC;
How many DBUs of each SKU have been used so far this month?
SELECT sku_name as `SKU`, sum(usage_quantity) as `DBUs`
 FROM system.billing.usage
WHERE
 month(usage_date) = month(CURRENT_DATE)
GROUP BY sku
ORDER BY `DBUs` DESC;
```

**Which 10 users consumed the most DBUs?**

```sql
SELECT identity_metadata.run_as as `User`,
sum(usage_quantity) as `DBUs`
 FROM system.billing.usage
GROUP BY identity_metadata.run_as
ORDER BY `DBUs` DESC
LIMIT 10; 
```

**Which Jobs consumed the most DBUs?**

```sql
SELECT usage_metadata.job_id as `Job ID`,
sum(usage_quantity) as `DBUs`
 FROM system.billing.usage
GROUP BY `Job ID`;
```

### System Tables: Audit Logs


**Who accesses this table the most?**

```sql
SELECT user_identity.email, count(*)
FROM system.access.audit
WHERE request_params.table_full_name =
"main.uc_deep_dive.login_data_silver"
AND service_name = "unityCatalog"
AND action_name = "generateTemporaryTableCredential"
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;
```


**What has this user accessed in the last 24 hours?**

```sql
SELECT request_params.table_full_name
FROM system.access.audit
WHERE user_identity.email = "ifi.derekli@databricks.com"
AND service_name = "unityCatalog"
AND action_name = "generateTemporaryTableCredential"
AND datediff(now(), event_time) < 1;
```

**Who deleted this table?**

```sql
SELECT user_identity.email
FROM system.access.audit
WHERE request_params.full_name_arg =
"main.uc_deep_dive.login_data_silver"
AND service_name = "unityCatalog"
AND action_name = "deleteTable";
```

**What tables does this user access most frequently?**

```sql
SELECT request_params.table_full_name, count(*)
FROM system.access.audit
WHERE user_identity.email = "ifi.derekli@databricks.com"
AND service_name = "unityCatalog"
AND action_name = "generateTemporaryTableCredential"
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;
```


### System Tables: Lineage Data

**What tables are sourced from this table?**

```sql
SELECT DISTINCT target_table_full_name
FROM system.access.table_lineage
WHERE source_table_name = "login_data_bronze";
```

**What user queries read from this table?**

```sql
SELECT DISTINCT entity_type, entity_id,
source_table_full_name
FROM system.access.table_lineage
WHERE source_table_name = "login_data_silver";
```

### Documentation

- [Table Lineage](https://docs.databricks.com/en/admin/system-tables/lineage.html)
- [Column Lineage](https://docs.databricks.com/en/admin/system-tables/lineage.html#column-lineage-table)

<hr/>

## Dynamic View

```sql
CREATE OR REPLACE VIEW customer_golden_view AS
       SELECT 
           -- anonymize customer_id for regular users
           CASE 
            WHEN 
            is_account_group_member('admins') THEN customer_id
            ELSE 99999
        END AS customer_id,
           state,
           AVG(units_purchased) AS avg_units_purchased,
           loyality_segment,
       FROM customers_silver_table
       WHERE
           -- filter table for regular users
           CASE 
            WHEN is_account_group_member('admins') THEN TRUE
            ELSE loyality_segment < 3
        END
        GROUP BY customer_id, state, loyality_segment
        ORDER BY customer_id;
```

```sql
DROP FUNCTION IF EXISTS loyality_segment_mask;

CREATE OR REPLACE FUNCTION loyality_segment_mask(loyality_segment INT)
       RETURNS BOOLEAN
       RETURN IF(is_account_group_member('admins'), TRUE, loyality_segment < 3);
       
DROP FUNCTION IF EXISTS customer_column_mask;

CREATE OR REPLACE FUNCTION customer_column_mask(customer_id INT)
       RETURNS BOOLEAN
       RETURN IF(is_account_group_member('admins'), TRUE, customer_id = 99999);

-- SET MASK ON ROW
ALTER TABLE customer_table 
SET ROW FILTER loyality_segment_mask ON loyality_segment;

-- SET MASK ON COLUMN
ALTER TABLE customer_table 
ALTER COLUMN customer_id 
      SET MASK loyality_segment_mask;
       
```

Taging

```sql
ALTER TABLE table_name
SET TAGS (
    'quality' = 'silver',
    'domain' = 'customer'
    );

ALTER TABLE table_name
ALTER COLUMN customer_id
      SET TAGS (
          'compliance' = 'GPDR'
      );
```

<hr/>

**Pseudonymization**

- Switches original data point with pseudonym for later re-identification
- Only authorized users will have access to keys/hash/table for re-identification
- Protects datasets on record level for machine learning
- A pseudonym is still considered to be personal data according to the GDPR
- Two main pseudonymization methods: hashing and tokenization

Method: **Hashing**

1. Apply SHA or other hash to all PII
2. Add random string "salt" to values before hashing
3. Databricks secrets can be leveraged for obfuscating salt value
4. Leads to some increase in data size
5. Some operations will be less efficient

Method: **Tokenization**
1. Converts all PII to keys
2. Values are stored in a secure lookup table
3. Slow to write, but fast to read
4. De-identified data stored in fewer bytes


Method: **Anonymization**
- Protects entire dataset (tables, databases or entire data catalogues) mostly for Business Intelligence
- Personal data is irreversibly altered in such a way that a data subject can no longer be identified directly or indirectly
- Usually a combination of more than one technique used in real-world scenarios
- Two main anonymization methods: data suppression and generalization

Method: **Data Suppression**
1. Exclude columns with PII from views
2. Remove rows where demographic groups are too small
3. Use dynamic access controls to provide conditional access to full data

Method: **Generalization**

1. Categorical generalization
2. Numerical generalization
3. Binning
4. Truncating IP addresses
5. Rounding


## Deleting data 

> In order to comply with privacy regulations like GDPR and CCPA, PII needs to be
effectively and efficiently handled in Databricks, and deleting PII in particular requires
special attention. Deletion is typically handled in pipelines that are separate from ETL
pipelines.
CDF data can be used to propagate deletion actions to downstream tables. This is
similar to filtering multiplex bronze tables to implement different downstream pipelines
for performing ETL, where we're ingesting data into a CDC feed.

Delta Lake supports **arbitrary commit messages** that will be recorded
to the Delta **transaction log** and viewable in the table history. This can
help with later auditing.

**Note**: When Delta Lake's history and CDF features are implemented,
deleted **PII values are still present in older versions of the data**.
• Using **Vacuum** command will physically delete PIIs
• **Deleting at a partition boundary** will make the whole process more
efficient

> By default, the Delta engine will prevent VACUUM operations with less
than 7 days of retention.

**To manually run VACUUM for these files:** 
1.  Disable Spark’s retention duration check (retentionDurationCheck.enabled)
2.  Run VACUUM with DRY RUN to preview files before permanently removing them
3.  Run VACUUM with RETAIN 0 HOURS!

DML works on streaming tables only

Updates, deletes, inserts and merges on streaming tables.

1.  Ensure compliance for retention periods on a table.
```sql
DELETE FROM my_live_tables.users WHERE updated < current_time() – INTERVAL 3 years;
```
2.  Scrub PII from data in the lake.
```sql
UPDATE my_live_tables.users SET email = hash(email, salt) WHERE id = 2;
```
3.  Append new data.
```sql
INSERT INTO my_live_tables.users VALUES (3, hash(email, salt), current_time());
```