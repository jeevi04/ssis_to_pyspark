# Transformation Rules

## General Rules

### Naming Conventions
- Function names: `snake_case`, descriptive of the transformation
- Variable names: `snake_case`
- DataFrame variables: suffix with `_df` (e.g., `customer_df`, `filtered_df`)
- Column references: use `F.col("column_name")` consistently

### NULL Handling
- SSIS treats empty strings and NULLs differently - be explicit
- Use `F.coalesce()` for NULL replacement
- Use `F.when(...).isNull()` for NULL checks
- Document NULL behavior in comments

### Column Types
- INPUT columns → columns from upstream components
- OUTPUT columns → columns in resulting DataFrame
- DERIVED columns → calculated columns (withColumn)
- PASSTHROUGH columns → columns that flow through unchanged

## Transformation-Specific Rules

### OLE DB Source / ADO.NET Source
- Convert SQL command to `spark.read.option("query", sql)`
- Handle source filters as DataFrame filter operations
- Map connection managers to JDBC connections

### Derived Column
- Each derived column becomes a `withColumn()` call
- Chain `withColumn()` calls or use `select()` with all expressions
- Handle expression functions (SUBSTRING, UPPER, DATEADD, etc.)

### Merge Join
- Map join type: Inner, Left Outer, Full Outer
- Convert SSIS join conditions to PySpark join conditions
- Note: SSIS Merge Join requires sorted inputs

### Lookup
- Use `broadcast()` for lookups under 100MB
- Handle multiple match scenarios
- Handle lookup miss with coalesce for default values

### Filter
- Direct mapping to DataFrame `filter()` or `where()`
- Convert SSIS boolean expressions to PySpark

### Aggregate
- GROUP BY columns → `groupBy()` columns
- Aggregate functions → `agg()` expressions
- Handle operations: Sum, Average, Count, Count Distinct, Min, Max

### Conditional Split
- Split into multiple DataFrames using `filter()`
- Name each output clearly
- Return as dictionary of DataFrames
- Handle default output for unmatched rows

### Sort
- Direct mapping to `orderBy()`
- Handle remove duplicates flag with `dropDuplicates()`
