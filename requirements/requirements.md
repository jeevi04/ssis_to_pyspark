# SSIS to PySpark Converter - Requirements

## Document Information
| Attribute | Value |
|-----------|-------|
| Document ID | REQ-001 |
| Version | 2.0 |
| Last Updated | 2025-01-02 |

---

## Overview

This document outlines the requirements for converting SQL Server Integration Services (SSIS) packages, Data Flow Tasks, and transformations to PySpark code. The converter supports the full lifecycle of SSIS artifacts including packages (.dtsx), project files (.dtproj), and individual Data Flow Tasks.

---

## Functional Requirements

### FR-001: DTSX Parsing
- The system shall parse SSIS package files (.dtsx)
- The system shall extract source definitions, destination definitions, and transformations
- The system shall handle nested Data Flow Task structures
- The system shall parse package definitions including Control Flow Tasks, Data Flow Tasks, and precedence constraints
- The system shall extract connection managers and parameter configurations
- The system shall support project-level deployments with multiple packages

### FR-002: Transformation Support
The system shall convert the following SSIS transformations:
- OLE DB Source / ADO.NET Source / Flat File Source
- OLE DB Destination / ADO.NET Destination / Flat File Destination
- Derived Column
- Conditional Split
- Aggregate
- Merge Join
- Lookup
- Multicast
- Sort
- Data Conversion
- Union All
- Row Count
- Script Component
- Slowly Changing Dimension
- OLE DB Command
- Fuzzy Lookup / Fuzzy Grouping (with manual intervention)

### FR-003: Package Support
- The system shall parse SSIS package definitions and execution order
- The system shall convert Control Flow logic to orchestration code
- The system shall handle package variables and parameters
- The system shall support precedence constraints with expressions
- The system shall handle event handlers and error paths

### FR-004: Data Flow Task Support
- The system shall extract Data Flow Task-level configurations
- The system shall handle component properties and custom properties
- The system shall convert buffer sizing and engine thread strategies
- The system shall handle connection manager assignments
- The system shall support Execute SQL Tasks as pre/post operations

### FR-005: Data Type Mapping
- The system shall map SSIS data types (DT_*) to PySpark equivalents
- The system shall preserve precision and scale for numeric types
- The system shall handle date/timestamp format conversions
- The system shall support complex types (DT_IMAGE, DT_NTEXT)

### FR-006: Expression Conversion
- The system shall convert SSIS Expression Language to PySpark
- The system shall handle built-in SSIS functions (SUBSTRING, TRIM, DATEADD, etc.)
- The system shall preserve business logic in Derived Column expressions
- The system shall convert Lookup component references
- The system shall handle variable references and expressions

### FR-007: Output Generation
- The system shall generate readable, well-formatted PySpark code
- The system shall include comments documenting the original SSIS components
- The system shall generate code that follows PySpark best practices
- The system shall generate workflow orchestration scripts (Airflow, Prefect, etc.)

---

## Non-Functional Requirements

The non-functional requirements are organized into detailed specification documents:

| NFR Category | Document | Specification Count |
|--------------|----------|---------------------|
| Performance | [NFR-Performance.md](nfr/NFR-Performance.md) | 115 specifications |
| Accuracy & Quality | [NFR-Accuracy.md](nfr/NFR-Accuracy.md) | 142 specifications |
| Maintainability | [NFR-Maintainability.md](nfr/NFR-Maintainability.md) | 125 specifications |
| Extensibility | [NFR-Extensibility.md](nfr/NFR-Extensibility.md) | 140 specifications |
| Security | [NFR-Security.md](nfr/NFR-Security.md) | 143 specifications |
| Usability | [NFR-Usability.md](nfr/NFR-Usability.md) | 128 specifications |
| Reliability | [NFR-Reliability.md](nfr/NFR-Reliability.md) | 126 specifications |
| Scalability | [NFR-Scalability.md](nfr/NFR-Scalability.md) | 125 specifications |
| Compatibility | [NFR-Compatibility.md](nfr/NFR-Compatibility.md) | 146 specifications |
| Documentation | [NFR-Documentation.md](nfr/NFR-Documentation.md) | 146 specifications |

### NFR Summary

#### Performance
- DTSX parsing: < 2s for small files, < 90s for 100MB files
- Single Data Flow Task conversion: < 30 seconds end-to-end
- Support concurrent processing of up to 5 Data Flow Tasks
- Memory usage: < 2GB for large file processing

#### Accuracy & Quality
- Transformation conversion accuracy: > 98%
- Data type mapping preservation: 100%
- Expression conversion accuracy: > 98%
- Generated code syntax validity: 100%

#### Security
- Encrypted API key storage
- No hardcoded credentials in generated code
- PII detection and masking in logs
- TLS 1.2+ for all API communications

#### Reliability
- Graceful handling of malformed DTSX
- Automatic retry with exponential backoff
- Checkpoint-based recovery for long conversions
- Zero data loss on failures

---

## Input Requirements

- Valid SSIS package files (.dtsx)
- Supported input types:
  - Individual package exports
  - Project-level exports (.dtproj / .ispac)
  - Solution-level exports
  - Package files with connection managers
  - Package deployment files
- Configuration file specifying LLM provider and settings
- Optional parameter files for connection and variable overrides

---

## Output Requirements

- PySpark Python files (.py)
- Workflow orchestration files (Airflow DAGs, etc.)
- Conversion logs and reports
- Validation results from evaluation framework
- Data lineage documentation
- Dependency graphs
- Manual intervention items list

---

## Domain Knowledge Requirements

The system shall leverage domain knowledge for accurate conversion. See:
- [knowledge/transformations/](../knowledge/transformations/) - Detailed transformation documentation
- [knowledge/type_mappings.yaml](../knowledge/type_mappings.yaml) - Type mapping reference
- [knowledge/pyspark_patterns.md](../knowledge/pyspark_patterns.md) - PySpark patterns

---

## Acceptance Criteria

1. All functional requirements must pass automated tests
2. All Critical NFR specifications must meet targets
3. Conversion accuracy validated against golden test suite
4. Generated code must execute without syntax errors
5. Documentation must be complete and accurate

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-02 | System | Initial version with complete NFR breakdown |
| 2.0 | 2025-01-02 | System | Migrated from Informatica to SSIS |
