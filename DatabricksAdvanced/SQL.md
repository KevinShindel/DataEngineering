

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


# UDF

```sql
CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
       RETURN STRING
       RETURN CONCAT('The ', item_name, ' is on the sale for a $', ROUND(item_price, 2))
       
SELECT *, sale_announcement(item,price) as message FROM item_lookup


CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
       RETURN STRING
       RETURN CASE
        WHEN name = 'Name1' THEN 0
        WHEN name = 'Name2' THEN 1
        WHEN name = 'Name3' THEN 2
        ELSE CONCAT(name, 'is not for sale')
       END;
```