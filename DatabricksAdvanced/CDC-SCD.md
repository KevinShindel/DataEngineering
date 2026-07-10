# [Change Data Capture](https://docs.databricks.com/aws/en/data-engineering/what-is-cdc)

### CDC Definition and Purpose
Change Data Capture is a technique used to track and capture changes in data sources like databases, Lakehouses, or data warehouses, and then apply those changes to a target table to ensure it reflects the latest state of the source.

**Slowly Changing Dimensions (SCDs)**

CDC is closely tied to the concept of Slowly Changing Dimensions (SCDs), which describe how historical data changes are handled in your target system.

We'll focus on two main types:

- SCD Type 1 - Overwrites existing data with new values (no history tracking)
- SCD Type 2 - Preserves history by storing previous versions of records

![CDC Explained](https://cloud.scorm.com/vault/d654995b-6321-4423-b8b5-d9d0c05fc1ef/content/courses/W6FCYNBK2T/scorm_module_cmp5clsyo03xu1upeduymhfv7/1/scormcontent/images/01-cdcoverview-review-b8acc9c22c.png)

```sql
-- CDC Type 1 example
CREATE OR REFRESH TABLE customers;
       
CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO customers
FROM STREAM updates
KEYS (CustomerID)
APPLY AS DELETE WHEN opetation = 'delete'
SEQUENCE BY ProcessedDate
COLUMNS * EXCEPT (operation)
STORED AS CDC TYPE 1;
```

```sql
-- CDC Type 2 example 
-- Create a streaming table, then use AUTO CDC to populate it:
CREATE OR REFRESH STREAMING TABLE target;

CREATE FLOW scd_type_2_flow
AS AUTO CDC INTO
  target
FROM STREAM cdc_data.users
  KEYS (userId)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum)
  STORED AS SCD TYPE 2
  TRACK HISTORY ON * EXCEPT (city);
```


```sql
MERGE INTO target_table_name as t
USING source_table_name as s
ON s.client_id = t.client_id
-- update statement
WHEN MATCHED AND s.tatus = 'update' THEN
    UPDATE SET
    t.email = s.email
    t.status = s.status
-- delete statement
WHEN MATCHED AND s.status = 'delete' THEN
    DELETE
-- insert statement
WHEN NOT MATCHED THEN
    INSERT (id, name, email, sing_up_date, status)
    VALUES (s.id, s.name, s.email, s.sing_up_date, s.status)
```

### If schema can be changed use schema evolution

```sql
MERGE WITH SCHEMA EVOLTUION target_table_name
....
```

### Decision Framework

**Choose SCD Type 1 when**:

+ Only current data state is needed
+ Storage efficiency is prioritized
+ Historical tracking is not required

**Choose SCD Type 2 when**:

+ Historical analysis is essential
+ Audit trails are required for compliance
+ Point-in-time reporting is needed

### Documentation 

1. [AUTO CDC API](https://docs.databricks.com/aws/en/ldp/cdc?language=SQL)
2. [AUTO CDC INTO](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-apply-changes-into)