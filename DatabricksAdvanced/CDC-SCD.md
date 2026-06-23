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