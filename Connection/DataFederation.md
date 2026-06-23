## Connection Setup by SQL

```sql
CREATE CONNECTION postgresql_uc 
        TYPE POSTGRESQL
       OPTIONS (
            host 'hostname',
            port 0000,
            user 'username',
            password 'secretpassword'
       )
       
CREATE FOREIGN CATALOG postgres_catalog
       USING CONNECTION postgresql_uc;
```