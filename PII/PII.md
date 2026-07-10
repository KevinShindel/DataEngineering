
```sql
CREATE OR REPLACE FUNCTION catalog.schema.pii_mask(col STRING)
       RETURN IF (is_account_group_member('admin'), col, 'MASKED')
       
ALTER TABLE postgres_catalog.public.online_users
    ALTER COLUMN lastname SET MASK catalog.schema.pii_mask;
```