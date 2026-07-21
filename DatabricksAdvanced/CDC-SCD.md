# [Change Data Capture](https://docs.databricks.com/aws/en/data-engineering/what-is-cdc)

<hr/> 

## CDC Definition and Purpose

Change Data Capture is a technique used to track and capture changes in data sources like databases, Lakehouses, or data warehouses,
and then apply those changes to a target table to ensure it reflects the latest state of the source.

<hr/> 

## **Slowly Changing Dimensions (SCDs)**

CDC is closely tied to the concept of Slowly Changing Dimensions (SCDs),
which describe how historical data changes are handled in your target system.

**We'll focus on two main types:**

- SCD Type 1 - Overwrites existing data with new values (no history tracking)
- SCD Type 2 - Preserves history by storing previous versions of records

<hr/> 

## CDC Explained

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

## If schema can be changed use schema evolution

```sql
MERGE WITH SCHEMA EVOLTUION target_table_name
....
```

<hr/>

## Decision Framework

**Choose SCD Type 1 when**:

+ Only current data state is needed
+ Storage efficiency is prioritized
+ Historical tracking is not required

**Choose SCD Type 2 when**:

+ Historical analysis is essential
+ Audit trails are required for compliance
+ Point-in-time reporting is needed

## Documentation 

1. [AUTO CDC API](https://docs.databricks.com/aws/en/ldp/cdc?language=SQL)
2. [AUTO CDC INTO](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-apply-changes-into)

<hr/>

## Comparison CDF vs CDC


| Feature        | Change Data Feed (CDF)                                                            | Change Data Capture (CDC)                                                 |
|----------------|-----------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| Scope          | Specific to Delta Lake tables                                                     | General concept applicable across systems                                 |
| Functionality  | Tracks row-level changes within Delta tables                                      | Captures data changes for synchronization across systems                  |
| Implementation | Enabled on Delta tables; uses `_change_data` folder and `table_changes` function. | Implemented using Spark Declarative Pipelines and APIs like APPLY CHANGES |
| Efficiency     | Processes only changed rows for operations.                                       | Synchronizes incremental changes from source databases                    |
| Use Case       | Tracking changes within Databricks                                                | Capturing changes from external sources                                   |

<hr/> 

## **CDF Configuration**

Important notes for CDF configuration
• CDF is **not enabled** by default. It can be enabled;
• At table level: **ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)**
• For all new tables: set **spark.databricks.delta.properties.defaults.enableChangeDataFeed = true**;
• Change feed can be read by;
• Version
• Timestamp
<hr/> 

## **Change Tables Function**

Tracks row-level changes between versions of a Delta table
- It returns a log of changes to a Delta Lake table with Change Data Feed enabled, including inserts, updates, and deletes
- Leverages metadata columns:
- - _change_type: Specifies the type of change (insert, delete, update_preimage, update_postimage)
- - _commit_version: The commit version associated with the change
- - _commit_timestamp: The timestamp of the change

<hr/> 

## **Syntax**

```sql
table_changes(table_name, start, [end])
```