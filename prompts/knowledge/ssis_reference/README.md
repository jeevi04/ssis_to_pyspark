# SSIS to PySpark - Knowledge Base Reference

Welcome to the comprehensive reference for converting SSIS packages to PySpark. This knowledge base is organized by architectural level and component type to facilitate easy maintenance and lookup.

## Structure

### 1. [Control Flow](control_flow/README.md)
Rules and patterns for converting SSIS Control Flow elements (Tasks, Containers, and Precedence Constraints) into Python logic.

- **Containers**: Loop, ForEach, and Sequence Containers.
- **Tasks**: Execute SQL, File System, Script Task, etc.
- **CDC Control**: Change Data Capture state management.

### 2. [Data Flow](data_flow/README.md)
Rules and patterns for converting Data Flow components into PySpark DataFrame operations.

- **[Sources](data_flow/sources/README.md)**: Extract components from databases, files, and more.
- **[Transformations](data_flow/transformations/README.md)**: Core, Fuzzy, and CDC transformations.
- **[Destinations](data_flow/destinations/README.md)**: Load components to databases and storage.

---

## General Patterns
- **[Bronze Patterns](../bronze_layer_patterns.md)**: Raw ingestion rules.
- **[Gold Patterns](../gold_layer_patterns.md)**: Promotion and aggregation rules.
- **[Orchestrator Patterns](../main_orchestrator_patterns.md)**: Main pipeline logic and validation.
- **[Variable Rules](../variable_rules.md)**: Mapping SSIS Variables.
- **[Connection Manager Rules](../connection_manager_rules.md)**: Handling configurations.
- **[Project Parameters](project_parameters.md)**: Handling Project-level parameters.
- **[Environment References](environment_references.md)**: Moving between DEV/TEST/PROD.
- **[Type Mappings](../type_mappings.yaml)**: Data type conversion.
- **[PySpark Patterns](../pyspark_patterns.md)**: General best practices.
