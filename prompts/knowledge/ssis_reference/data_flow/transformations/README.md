# SSIS Transformations - Knowledge Base

This directory contains comprehensive documentation for converting SSIS (SQL Server Integration Services) transformations to PySpark code.

## Overview

SSIS provides a rich set of data flow transformations for ETL operations. This knowledge base documents how to convert each SSIS transformation type to equivalent PySpark DataFrame operations.

## Complete List of Data Flow Components

### 1️⃣ SOURCES (Extract Components)
Refer to [sources.md](../sources/sources.md) for details.

- **OLE DB Source**
- **ADO.NET Source**
- **ODBC Source**
- **Flat File Source**
- **Excel Source**
- **XML Source**
- **Raw File Source**
- **DataReader Source**
- **CDC Source**
- **SharePoint List Source**

### 2️⃣ TRANSFORMATIONS (Transform Components)

#### 🔹 Core Transformations
- **[Aggregate](aggregator.md)** - Perfrom aggregations (SUM, AVG, COUNT, etc.)
- **[Audit](row_count.md)** - Add system/package information
- **[Cache Transform](core_utility.md)** - Write data to cache for Lookup
- **[Character Map](core_utility.md)** - Apply string functions (upper, lower, etc.)
- **[Conditional Split](conditionalsplit.md)** - Route rows based on conditions
- **[Copy Column](core_utility.md)** - Create copies of input columns
- **[Data Conversion](data_conversion.md)** - Convert data types
- **[Derived Column](expression.md)** - Create/modify columns using expressions
- **[Export Column](core_utility.md)** - Export data to files
- **[Import Column](core_utility.md)** - Import data from files
- **[Lookup](lookup.md)** - Join with reference datasets
- **[Merge](merge.md)** - Combine two sorted datasets
- **[Merge Join](mergejoin.md)** - Join two sorted datasets
- **[Multicast](multicast.md)** - Send data to multiple outputs
- **[OLE DB Command](oledb_command.md)** - Run SQL for each row
- **[Percentage Sampling](sampling.md)** - Sample a percentage of rows
- **[Pivot](pivot.md)** - Convert rows to columns
- **[Row Count](row_count.md)** - Count rows into a variable
- **[Row Sampling](sampling.md)** - Sample a specific number of rows
- **[Script Component](script_component.md)** - Custom C#/VB.NET logic
- **[Slowly Changing Dimension](update_strategy.md)** - Handle SCD Type 1 & 2
- **[Sort](sorter.md)** - Sort data
- **[Term Extraction](text_analysis.md)** - Extract terms from text
- **[Term Lookup](text_analysis.md)** - Match terms against reference
- **[Union All](union.md)** - Combine multiple datasets
- **[Unpivot](normalizer.md)** - Convert columns to rows
- **[Balanced Data Distributor](balanced_data_distributor.md)** - Repartition data for parallel processing

#### 🔹 Fuzzy Transformations
- **[Fuzzy Grouping](fuzzy.md)** - Identify similar rows
- **[Fuzzy Lookup](fuzzy.md)** - Approximate matching lookup

#### 🔹 CDC Transformation
- **[CDC Splitter](cdc.md)** - Route CDC operations


### 3️⃣ DESTINATIONS (Load Components)
Refer to [destinations.md](../destinations/destinations.md) for details.

- **OLE DB Destination**
- **SQL Server Destination**
- **ADO.NET Destination**
- **ODBC Destination**
- **Flat File Destination**
- **Excel Destination**
- **Raw File Destination**
- **Recordset Destination**

## Conversion Summary Table

| SSIS Transformation | PySpark Equivalent | Knowledge File |
|---------------------|-------------------|----------------|
| Aggregate | `groupBy().agg()` | [aggregator.md](aggregator.md) |
| Derived Column | `withColumn()`, expressions | [expression.md](expression.md) |
| Conditional Split | `filter()`, multiple outputs | [conditionalsplit.md](conditionalsplit.md) |
| Merge Join | `join()` | [mergejoin.md](mergejoin.md) |
| Lookup | `join()` with broadcast | [lookup.md](lookup.md) |
| Sort | `orderBy()` | [sorter.md](sorter.md) |
| Union All | `union()` | [union.md](union.md) |
| Data Conversion | `.cast()` | [data_conversion.md](data_conversion.md) |
| Multicast | Cache + multiple references | [multicast.md](multicast.md) |
| Pivot | `.pivot()` | [pivot.md](pivot.md) |
| Unpivot | `stack()`, `melt()` | [normalizer.md](normalizer.md) |
| Row Count | `.count()` | [row_count.md](row_count.md) |
| Script Component | UDFs, Pandas UDFs | [script_component.md](script_component.md) |
| Merge | `union() + orderBy()` | [merge.md](merge.md) |
| OLE DB Command | Batch operations, Delta merge | [oledb_command.md](oledb_command.md) |
| Slowly Changing Dimension | Delta merge, SCD patterns | [update_strategy.md](update_strategy.md) |


## Usage

Each transformation knowledge file contains:
- **Purpose**: Description of the SSIS transformation
- **Conversion Pattern**: Template for PySpark conversion
- **Example Conversions**: Practical examples
- **Function Mappings**: SSIS to PySpark function mappings
- **Key Properties**: Important SSIS properties to handle
- **Performance Considerations**: Optimization tips

## Data Type Mappings

See [type_mappings.yaml](../../../type_mappings.yaml) for comprehensive SSIS/SQL Server to PySpark data type mappings.

## PySpark Best Practices

See [pyspark_patterns.md](../../../pyspark_patterns.md) for general PySpark coding patterns and best practices.

## Contributing

When adding new transformation knowledge:
1. Follow the existing template structure
2. Include practical examples
3. Document performance considerations
4. Map SSIS-specific properties to PySpark equivalents
5. Include error handling patterns where applicable
