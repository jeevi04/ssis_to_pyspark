# SSIS Data Flow - Knowledge Base

This section documents how to convert SSIS Data Flow components into PySpark DataFrame operations.

## Sections

- **[Sources](sources/sources.md)**: Extracting data from various systems.
- **[Transformations](transformations/README.md)**: Manipulating data in-flight.
- **[Destinations](destinations/destinations.md)**: Loading data to target systems.

## Data Flow Architecture in PySpark
The SSIS Data Flow engine is row-based (with some buffer optimizations). PySpark is set-based and distributed.

### Key Mapping Principles:
1. **Pipeline to Functions**: Each Data Flow Task is generally converted to a Python function using the naming convention `transform_{prefix}_{name}` (e.g., `transform_dft_mydataflow`).
2. **Buffer to DataFrame**: Data flowing between components is represented as a PySpark DataFrame.
3. **Component to Operation**: Each SSIS component (Source, Transformation, Destination) maps to one or more PySpark operations. **Every component must be represented by a function to ensure traceability.**
4. **Preservation**: Unlike some ETL tools, SSIS transformations pass all columns through by default unless explicitly removed. PySpark DataFrames follow this same principle.

## Data Quality & Validation Patterns

### Validation Identifier Rule (CRITICAL)
When implementing validation logic (mapped from SSIS Row Count or Script components), avoid hardcoded or generic column names like `entity_id` or `update_date`. **Always use the actual SSIS identifiers/business keys** (e.g., `AddressIdentifier`, `SourceUpdateTS`) explicitly present in the SSIS source mappings. This ensures the output maintains traceability to the source system.

```python
def validate_data(df: DataFrame):
    # Check required fields based on SSIS source mappings
    # WRONG: F.col("entity_id").isNull()
    # RIGHT: F.col("AddressIdentifier").isNull()
    invalid_rows = df.filter(F.col("AddressIdentifier").isNull())
```

## Performance & Optimization
- **Caching**: Use `.cache()` for DataFrames branching into multiple outputs (e.g., Multicast).
- **Partitioning**: Ensure joins (Merge Join) use optimized partitioning if data volume is high.
