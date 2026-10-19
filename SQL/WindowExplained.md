## Window Function Params Explained

```sql
SELECT 
            LAST_VALUE(val) OVER(
            PARTITION BY id  -- window partitioning
            ORDER BY val -- ordering of rows within the partition
             ROWS BETWEEN UNBOUNDED PRECEDING -- window frame specification ( default is RANGE BETWEEN UNBOUNDED PRECEDING )
              AND
               UNBOUNDED FOLLOWING -- window frame specification ( default is RANGE BETWEEN UNBOUNDED PRECEDING )
               ) AS 'last_value'
        FROM numbers
```

Some Common Specifications
- UNBOUNDED PRECEDING: All rows before current row are considered.
- UNBOUNDED FOLLOWING: All rows after the current row are considered.
- CURRENT ROW: Range starts or ends at CURRENT ROW.

| Parameters                                       | Description                                                                |
|--------------------------------------------------|----------------------------------------------------------------------------| 
| ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW | includes the current row and all rows before current row                   |
| ROWS BETWEEN N PRECEDING AND CURRENT ROW         | includes the current row and a specified number of rows before current row |
| ROWS BETWEEN CURRENT ROW AND CURRENT ROW         | includes only the current row                                              |
| ROWS BETWEEN N PRECEDING AND M FOLLOWING         | includes specified rows before and after the current row                   |
| ROWS BETWEEN CURRENT ROW AND M FOLLOWING         | includes the current row and a specified number of rows after current row  |
| ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING | includes the current row and all rows after current row                    |

## Non-Aggregate Window Functions Explained

- [CUME_DIST](#CUME_DIST-Explained) - Cumulative Distribution
- [DENSE_RANK](#DENSE_RANK-Explained) - Rank of current row within its partition, without gaps
- [FIRST_VALUE](#FIRST_VALUE-Explained) - Value of argument from first row of window frame
- [LAG](#LAG-Explained) - Previous value from row within partition
- [LAST_VALUE](#LAST_VALUE-Explained) - Value of argument from last row of window frame
- [LEAD](#LEAD-Explained) - Next value from row within partition
- [NTH_VALUE](#NTH_VALUE-Explained) - Value of argument from nth row of window frame
- [NTILE](#NTILE-Explained) - Divides the window into a specified number of roughly equal parts and assigns a unique bucket number to each row within the partition
- [PERCENT_RANK](#PERCENT_RANK-Explained) - Relative rank of current row within its partition, expressed as a percentage
- [RANK](#RANK-Explained) - Rank of current row within its partition, with gaps
- [ROW_NUMBER](#ROW_NUMBER-Explained) - Unique number

<hr />

## CUME_DIST Explained

**Description**:
> The CUME_DIST function calculates the cumulative distribution of a value within a partition. It returns a value between 0 and 1, representing the proportion of rows that have a value less than or equal to the current row's value.

```sql
SELECT
            val,
            ROW_NUMBER() OVER(ORDER BY val) AS 'row_number',
            CUME_DIST() OVER(ORDER BY val) AS 'cume_dist'
          FROM numbers
```

| val    | row_number   | cume_dist |
|--------|--------------|-----------|
| 1      | 1            | 0.2222    |
| 1      | 2            | 0.2222    |
| 2      | 3            | 0.3333    |
| 3      | 4            | 0.5556    |
| 3      | 5            | 0.5556    |
| 4      | 6            | 0.5556    | 
| 5      | 7            | 0.7778    |
| 6      | 8            | 0.8889    |
| 7      | 9            | 1.0000    |


## DENSE_RANK Explained

**Description**:
> The DENSE_RANK function assigns a rank to each row within a partition of a result set, with no gaps in ranking values. Rows with equal values receive the same rank.


```sql
SELECT
            val,
            ROW_NUMBER() OVER(ORDER BY val) AS 'row_number',
            DENSE_RANK() OVER(ORDER BY val) AS 'dense_rank'
          FROM numbers  
```

| val    | row_number   | dense_rank |
|--------|--------------|------------|
| 1      | 1            | 1          |
| 1      | 2            | 1          |
| 2      | 3            | 2          |  
| 3      | 4            | 3          |
| 3      | 5            | 3          |
| 4      | 6            | 4          |


## FIRST_VALUE Explained

**Description**:
> The FIRST_VALUE function returns the value of the argument from the first row of the window frame. The first row is determined based on the order of the rows within the partition.


```sql
SELECT 
         val,
         ROW_NUMBER() OVER(ORDER BY val) AS 'row_number',
         FIRST_VALUE(val) OVER(ORDER BY val) AS 'first_value'
       FROM numbers
```

| val    | row_number   | first_value |
|--------|--------------|-------------|
| 1      | 1            | 1           |
| 1      | 2            | 1           |
| 2      | 3            | 1           |
| 3      | 4            | 1           |
| 3      | 5            | 1           |

## LAG Explained

**Description**:
> The LAG function provides access to a value in a previous row within the same result set, without the use of a self-join. It allows you to look back a specified number of rows and retrieve the value from that row.


```sql
SELECT
         val,
         ROW_NUMBER() OVER(ORDER BY val) AS 'row_number',
         LAG(val, 1) OVER(ORDER BY val) AS 'lag_value'
       FROM numbers
```

| val    | row_number   | lag_value |
|--------|--------------|-----------|
| 1      | 1            | NULL      |
| 1      | 2            | 1         |
| 2      | 3            | 1         |   
| 3      | 4            | 2         |
| 3      | 5            | 3         |

## NTH_VALUE Explained

**Description**:
> The NTH_VALUE function returns the value of the argument from the nth row of the window frame. The nth row is determined based on the order of the rows within the partition.

```sql
SELECT 
         val,
         ROW_NUMBER() OVER(ORDER BY val) AS 'row_number',
         NTH_VALUE(val, 3) OVER(ORDER BY val) AS 'nth_value'
       FROM numbers
```

| val    | row_number   | nth_value |
|--------|--------------|-----------|
| 1      | 1            | 3         |
| 1      | 2            | 3         |
| 2      | 3            | 3         |   
| 3      | 4            | 3         |

## NTILE Explained

**Description**:
> The NTILE function divides the ordered rows in a partition into a specified number of groups,
or "tiles," and assigns a unique bucket number to each row based on its position within the ordered set.
The number of tiles is specified as an argument to the function.

```sql
SELECT
         val,
         ROW_NUMBER() OVER(ORDER BY val) AS 'row_number',
         NTILE(2)     OVER(ORDER BY val) AS 'ntile2',
         NTILE(4)     OVER(ORDER BY val) AS 'ntile4'
       FROM numbers
```

| val    | row_number   | ntile2   | ntile4   |
|--------|--------------|----------|----------|
| 1      | 1            | 1        | 1        |
| 1      | 2            | 1        | 1        |
| 2      | 3            | 1        | 1        |
| 3      | 4            | 1        | 2        |
| 3      | 5            | 1        | 2        |
| 3      | 6            | 2        | 3        |
| 4      | 7            | 2        | 3        |
| 4      | 8            | 2        | 4        |
| 5      | 9            | 2        | 4        |


## Window Aggregate Functions Explained

| Function                      | Description                                                       | Example |
|-------------------------------|-------------------------------------------------------------------|---------|
| AVG()                         | Average value of argument within window frame                     |         |
| BIT_AND()                     | Bitwise AND of argument within window frame                       |         |
| BIT_OR()                      | Bitwise OR of argument within window frame                        |         |
| BIT_XOR()                     | Bitwise XOR of argument within window frame                       |         |
| COUNT()                       | Count of non-NULL values of argument within window frame          |         |
| JSON_ARRAYAGG()               | Aggregates values into a JSON array within window frame           |         |
| JSON_OBJECTAGG()              | Aggregates key-value pairs into a JSON object within window frame |         |
| MAX()                         | Maximum value of argument within window frame                     |         |
| MIN()                         | Minimum value of argument within window frame                     |         |
| STDDEV_POP(), STDDEV(), STD() | Population standard deviation of argument within window frame     |         |
| STDDEV_SAMP()                 | Sample standard deviation of argument within window frame         |         |
| SUM()                         | Sum of argument within window frame                               |         |
| VAR_POP(), VARIANCE()         | Population variance of argument within window frame               |         |
| VAR_SAMP()                    | Sample variance of argument within window frame                   |         |