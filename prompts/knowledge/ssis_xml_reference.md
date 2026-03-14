# SSIS XML Parsing Reference

This document maps SSIS `.dtsx` XML elements to PySpark conversion rules used by the `SSISParser`.

## Overview

SSIS packages are XML files with a specific schema. The parser extracts key elements and converts them to PySpark equivalents.

## XML Element Mapping Table

| SSIS XML Section | XML XPath / Element | Rule Action |
|---|---|---|
| **Package properties** | `DTS:Executable[@DTS:ExecutableType='SSIS.Package.3']` | Extract package name, description |
| **Connection Managers** | `DTS:ConnectionManager` | Generate SparkSession / JDBC configs |
| **Variables** | `DTS:Variable` | Generate Python variables / constants |
| **Control Flow tasks** | `DTS:Executable` (children of package) | Generate sequential function calls |
| **Precedence Constraints** | `DTS:PrecedenceConstraint` | Generate if/else or try/except flow |
| **Data Flow pipeline** | `DTS:Executable[@DTS:ExecutableType='SSIS.Pipeline.3']` | Generate Spark DataFrame pipeline |
| **Data Flow components** | `pipeline:components/component` | Generate read/transform/write calls |
| **Event Handlers** | `DTS:EventHandler` | Generate try/except or logging hooks |
| **Configurations** | `DTS:Configuration` | Generate config dict or argparse |

## Namespace Prefixes

SSIS XML uses multiple namespaces:

```python
NAMESPACES = {
    'DTS': 'www.microsoft.com/SqlServer/Dts',
    'pipeline': 'microsoft.com/SqlServer/Dts/Pipeline',
    'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask',
}
```

## Key XPath Patterns

### Extract Package Name
```python
package_name = root.get('{www.microsoft.com/SqlServer/Dts}ObjectName', 'Unknown')
```

### Find All Data Flow Tasks
```python
data_flows = root.findall(
    ".//DTS:Executable[@DTS:ExecutableType='SSIS.Pipeline.3']",
    namespaces=NAMESPACES
)
```

### Extract Components from Data Flow
```python
components = data_flow.findall(
    ".//pipeline:components/pipeline:component",
    namespaces=NAMESPACES
)
```

### Get Component Type
```python
component_type = component.get('componentClassID', '')
# Example: 'DTSAdapter.OleDbSource.3' → OleDbSource
```

### Extract Input/Output Columns
```python
# Input columns
input_cols = component.findall(
    ".//pipeline:inputColumn",
    namespaces=NAMESPACES
)

# Output columns
output_cols = component.findall(
    ".//pipeline:outputColumn",
    namespaces=NAMESPACES
)
```

### Get Column Properties
```python
col_name = column.get('name')
col_datatype = column.get('dataType')  # SSIS type code (e.g., 'wstr', 'i4')
col_length = column.get('length')
col_precision = column.get('precision')
col_scale = column.get('scale')
```

### Extract Expressions (Derived Column)
```python
expression = column.get('expression', '')
friendly_expr = column.get('friendlyExpression', '')
```

### Get Connection Manager Reference
```python
conn_ref = component.find(
    ".//pipeline:connection[@connectionManagerRefId]",
    namespaces=NAMESPACES
)
conn_id = conn_ref.get('connectionManagerRefId') if conn_ref else None
```

## Component Type Mapping

| SSIS Component ClassID | Parsed Type | PySpark Equivalent |
|---|---|---|
| `DTSAdapter.OleDbSource` | `OleDbSource` | `spark.read.jdbc()` |
| `DTSAdapter.OleDbDestination` | `OleDbDestination` | `df.write.jdbc()` |
| `DTSAdapter.FlatFileSource` | `FlatFileSource` | `spark.read.csv()` |
| `DTSTransform.DerivedColumn` | `DerivedColumn` | `df.withColumn()` |
| `DTSTransform.Lookup` | `Lookup` | `df.join()` with broadcast |
| `DTSTransform.ConditionalSplit` | `ConditionalSplit` | `df.filter()` + multiple outputs |
| `DTSTransform.Aggregate` | `Aggregate` | `df.groupBy().agg()` |
| `DTSTransform.Sort` | `Sort` | `df.orderBy()` |
| `DTSTransform.MergeJoin` | `MergeJoin` | `df.join()` |
| `DTSTransform.UnionAll` | `UnionAll` | `df.union()` |
| `DTSTransform.Multicast` | `Multicast` | `df.cache()` + multiple refs |

## Data Type Mapping

SSIS uses numeric data type codes. Common mappings:

| SSIS Type Code | SSIS Type Name | PySpark Type |
|---|---|---|
| `bool` | Boolean | `BooleanType` |
| `i1` | TinyInt | `ByteType` |
| `i2` | SmallInt | `ShortType` |
| `i4` | Int | `IntegerType` |
| `i8` | BigInt | `LongType` |
| `r4` | Single | `FloatType` |
| `r8` | Double | `DoubleType` |
| `numeric` | Numeric | `DecimalType` |
| `wstr` | Unicode String | `StringType` |
| `str` | ANSI String | `StringType` |
| `dbDate` | Date | `DateType` |
| `dbTimeStamp` | DateTime | `TimestampType` |

## Parser Implementation

The `SSISParser` class in `src/parsers/ssis_dtsx.py` implements this mapping:

1. **Parse XML** → `xml.etree.ElementTree`
2. **Extract Package Metadata** → `Workflow` object
3. **Find Data Flow Tasks** → `Mapping` objects
4. **Parse Components** → `Transformation` objects
5. **Extract Columns** → `TransformField` objects
6. **Build Lineage** → `Connector` objects

## Example XML Structure

```xml
<Executable
    DTS:ExecutableType="SSIS.Pipeline.3"
    DTS:ObjectName="Data Flow Task">
    
    <ObjectData>
        <pipeline:components>
            <pipeline:component
                name="OLE DB Source"
                componentClassID="DTSAdapter.OleDbSource.3">
                
                <pipeline:outputs>
                    <pipeline:output>
                        <pipeline:outputColumns>
                            <pipeline:outputColumn
                                name="CustomerID"
                                dataType="i4"
                                length="0"
                                precision="0"
                                scale="0"/>
                        </pipeline:outputColumns>
                    </pipeline:output>
                </pipeline:outputs>
            </pipeline:component>
        </pipeline:components>
    </ObjectData>
</Executable>
```

## References

- **SSIS XML Schema**: Microsoft SQL Server Integration Services DTD
- **Parser Implementation**: `src/parsers/ssis_dtsx.py`
- **Type Mappings**: `knowledge/type_mappings.yaml`
- **Transformation Templates**: `knowledge/transformations/`
