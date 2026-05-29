

# Create table by file

```sql
CREATE TABLE IF NOT EXISTS sales_csv
    (order_id LONG,
     email STRING,
     transaction_ts INTEGER,
     revenue DOUBLE,
     items INTEGER)
USING csv
OPTIONS (
    header = 'true',
    delimiter = '|'
        )
LOCATION "${path.sales.csv}"
```

# Extract data from external SQL DB

```sql
CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
    url="jdbc:sqlite:${folder.to.path.ecommerce_db}",
    dbtable="users"
)
```