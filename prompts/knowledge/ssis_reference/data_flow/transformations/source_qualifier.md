# OLE DB Source / ADO.NET Source (SSIS)

## Purpose
The SSIS OLE DB Source and ADO.NET Source components extract data from relational databases using SQL queries or table names. They are the starting point of data flow in SSIS packages.

## Conversion Pattern

```python
def source_{source_name}(spark: SparkSession) -> DataFrame:
    """
    SSIS OLE DB/ADO.NET Source: {source_name}
    
    Connection: {connection_string}
    Access Mode: {access_mode}
    Table/Query: {table_or_query}
    """
    from pyspark.sql import functions as F
    
    # Read from database
    {read_logic}
    
    return result_df
```

## Access Mode Mapping

| SSIS Access Mode | PySpark Equivalent |
|------------------|-------------------|
| `Table or view` | `spark.read.table("table_name")` or JDBC read |
| `Table name or view name variable` | `spark.read.table(table_var)` |
| `SQL command` | `spark.read.jdbc(..., query="SELECT ...")` |
| `SQL command from variable` | `spark.read.jdbc(..., query=sql_var)` |

## Example Conversions

**Table Access Mode:**
```python
# SSIS: OLE DB Source - Table "Customers"
# For Hive/Delta tables
customers_df = spark.read.table("Customers")

# For SQL Server via JDBC
customers_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://server:1433;databaseName=MyDB") \
    .option("dbtable", "Customers") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
```

**SQL Command Access Mode:**
```python
# SSIS: OLE DB Source - SQL Command
sql_query = """
    SELECT CustomerID, CustomerName, Region, OrderAmount
    FROM Customers
    WHERE Region = 'North'
    AND OrderAmount > 1000
"""

# For SQL Server via JDBC
customers_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://server:1433;databaseName=MyDB") \
    .option("query", sql_query) \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
```

**Parameterized Query:**
```python
# SSIS: SQL Command with parameters
region_param = "North"
amount_param = 1000

sql_query = f"""
    SELECT CustomerID, CustomerName, Region, OrderAmount
    FROM Customers
    WHERE Region = '{region_param}'
    AND OrderAmount > {amount_param}
"""

customers_df = spark.read.jdbc(
    url="jdbc:sqlserver://server:1433;databaseName=MyDB",
    table=f"({sql_query}) AS subquery",
    properties={
        "user": "username",
        "password": "password",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
)
```

## Connection String Mapping

**SQL Server:**
```python
jdbc_url = "jdbc:sqlserver://server:1433;databaseName=MyDB"
properties = {
    "user": "username",
    "password": "password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
```

**Oracle:**
```python
jdbc_url = "jdbc:oracle:thin:@server:1521:SID"
properties = {
    "user": "username",
    "password": "password",
    "driver": "oracle.jdbc.driver.OracleDriver"
}
```

**MySQL:**
```python
jdbc_url = "jdbc:mysql://server:3306/database"
properties = {
    "user": "username",
    "password": "password",
    "driver": "com.mysql.jdbc.Driver"
}
```

## Performance Optimizations

**Partitioning for Parallel Reads:**
```python
# SSIS: Data access mode with partitioning
customers_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "Customers") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("partitionColumn", "CustomerID") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000000") \
    .option("numPartitions", "10") \
    .load()
```

**Column Pruning:**
```python
# Only read required columns
customers_df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT CustomerID, CustomerName, Region FROM Customers) AS subquery",
    properties=properties
)
```

**Predicate Pushdown:**
```python
# Push filter to database
customers_df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT * FROM Customers WHERE Region = 'North') AS subquery",
    properties=properties
)
```

## Key Properties to Handle
- **Connection Manager**: Database connection string
- **Access Mode**: Table, View, or SQL Command
- **Table Name**: Source table or view
- **SQL Command Text**: Custom SQL query
- **Command Timeout**: Query timeout (map to JDBC fetchsize)
- **Parameters**: SQL query parameters

## Data Type Mapping (SQL Server to PySpark)

| SQL Server Type | PySpark Type |
|-----------------|--------------|
| `INT`, `SMALLINT`, `TINYINT` | `IntegerType()` |
| `BIGINT` | `LongType()` |
| `DECIMAL`, `NUMERIC` | `DecimalType(p, s)` |
| `FLOAT`, `REAL` | `DoubleType()` |
| `VARCHAR`, `NVARCHAR`, `CHAR`, `NCHAR` | `StringType()` |
| `DATE` | `DateType()` |
| `DATETIME`, `DATETIME2`, `SMALLDATETIME` | `TimestampType()` |
| `BIT` | `BooleanType()` |
| `VARBINARY`, `BINARY` | `BinaryType()` |

## UNION ALL Source Queries

When the SSIS source SQL command uses `UNION ALL` to combine data from multiple tables, the PySpark code MUST reconstruct this using `.union()` or `.unionByName()` — **not** by reading from a non-existent pre-combined table.

> **NEVER read from a table that doesn't exist in the upstream Silver/Bronze pipeline. If the SSIS source SQL constructs data via UNION ALL, build it in PySpark.**

**SSIS Pattern:**
```sql
-- SSIS OLE DB Source SQL Command:
SELECT member_id, YEAR(service_date_start) AS encounter_year,
       'MEDICAL' AS encounter_type, service_date_start AS encounter_date, 1 AS encounter_count
FROM dbo.claims_transformed
UNION ALL
SELECT member_id, YEAR(fill_date) AS encounter_year,
       'PHARMACY' AS encounter_type, fill_date AS encounter_date, 1 AS encounter_count
FROM dbo.pharmacy_transformed
```

**Correct PySpark Translation:**
```python
def build_patient_journey_source(spark: SparkSession) -> DataFrame:
    """
    Reconstructs the SSIS UNION ALL source query from Silver layer tables.
    Combines medical claims and pharmacy data into a unified encounter view.
    """
    # Medical encounters from Silver claims
    medical_df = spark.table("silver.claims").select(
        F.col("member_id"),
        F.year(F.col("service_date_start")).alias("encounter_year"),
        F.lit("MEDICAL").alias("encounter_type"),
        F.col("service_date_start").alias("encounter_date"),
        F.lit(1).alias("encounter_count")
    )

    # Pharmacy encounters from Silver pharmacy
    pharmacy_df = spark.table("silver.pharmacy").select(
        F.col("member_id"),
        F.year(F.col("fill_date")).alias("encounter_year"),
        F.lit("PHARMACY").alias("encounter_type"),
        F.col("fill_date").alias("encounter_date"),
        F.lit(1).alias("encounter_count")
    )

    # UNION ALL — preserve encounter_type tag
    combined_df = medical_df.unionByName(pharmacy_df)
    return combined_df
```

**Key Rules:**
1. Parse the SQL command text from the SSIS source to identify UNION ALL clauses
2. Map each SELECT in the UNION to a `.select()` from the corresponding Silver table
3. Preserve ALL columns including type tags (e.g., `encounter_type = 'MEDICAL'`)
4. Use `.unionByName()` for schema safety
5. Column names and types must match across all union branches

## Destination Column Projection

> **The output DataFrame must contain ONLY the columns defined in the SSIS destination column mapping — not all upstream columns.**

SSIS destinations explicitly define which columns are written. If the destination maps only 3 columns (e.g., `member_id`, `gender_cd`, `zip_code`), the PySpark output must `.select()` only those 3 columns.

**WRONG — writes all 17+ upstream columns:**
```python
# All source columns + derived columns pass through
final_df.write.mode("overwrite").saveAsTable("silver.members")
```

**CORRECT — projects only destination columns:**
```python
# Select only the columns defined in the SSIS destination mapping
output_df = final_df.select(
    F.col("member_id"),
    F.col("gender_standardized").alias("gender_cd"),
    F.col("zip_code_standardized").alias("zip_code")
)
output_df.write.mode("overwrite").saveAsTable("silver.members")
```
