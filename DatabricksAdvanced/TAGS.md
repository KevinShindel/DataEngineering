
```sql
ALTER TABLE catalog_name.schema_name.table_name
SET TAGS (
    'demo_tag_Department' = 'Sales',
    'demo_tag_Quality' = 'bronze',
    'system.Certified' = 'true'
    )
```

## Search by tag

```sql
SELECT catalog_name, 
        schema_name,
        table_name,
        tag_name,
        tag_value
FROM information_schema.table_tags;
```