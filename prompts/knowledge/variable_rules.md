# Variable Conversion Rules

This document defines how SSIS Variables (`DTS:Variable`) are converted to Python variables in PySpark code.

## Overview

`DTS:Variable` elements map directly to Python variables at the top of the generated script. All variables are in scope for the entire package unless inside a container (scoped as local variables inside a function).

## 3. Variable Mapping Table

| SSIS XML Attribute / Element | XPath | PySpark Output (RULE) |
|---|---|---|
| **Variable name** | `DTS:Variable/@DTS:ObjectName` | Use as Python identifier (sanitized) |
| **Namespace = User** | `@DTS:Namespace='User'` | Module-level variable |
| **Namespace = System** | `@DTS:Namespace='System'` | Read from Spark context # SKIP most |
| **DataType = 3 (Int32)** | `@DTS:DataType='3'` | `int({value})` |
| **DataType = 8 (String)** | `@DTS:DataType='8'` | `"{value}"` |
| **DataType = 11 (Boolean)** | `@DTS:DataType='11'` | `True` / `False` |
| **DataType = 7 (Date)** | `@DTS:DataType='7'` | `datetime.strptime('{value}', ...)` |
| **DataType = 14 (Decimal)** | `@DTS:DataType='14'` | `Decimal('{value}')` |
| **DataType = 5 (Double)** | `@DTS:DataType='5'` | `float({value})` |
| **DataType = 13 (Object)** | `@DTS:DataType='13'` | `None` # LLM PATCH – object type |
| **EvaluateAsExpression=True** | `DTS:VariableValue@EvaluateAsExpression` | # LLM PATCH – evaluate expression: {expr} |
| **Expression value** | `DTS:VariableValue` text | Extract expression for LLM PATCH |
| **ReadOnly=True** | `@DTS:ReadOnly='True'` | CONSTANT (uppercase name) |
| **Scope = Package** | parent is Package | Module-level |
| **Scope = Container** | parent is Foreach/For/Sequence | Local variable inside function |

## Data Type Mapping

### SSIS DataType Codes

| Code | SSIS Type | Python Type | Example |
|---|---|---|---|
| 3 | Int32 | `int` | `batch_size = 1000` |
| 5 | Double | `float` | `threshold = 0.95` |
| 7 | Date | `datetime` | `start_date = datetime(2024, 1, 1)` |
| 8 | String | `str` | `environment = "production"` |
| 11 | Boolean | `bool` | `enable_logging = True` |
| 14 | Decimal | `Decimal` | `tax_rate = Decimal('0.0825')` |
| 13 | Object | `None` or custom | `config_obj = None` |

## Variable Examples

### Simple User Variables

**SSIS XML:**
```xml
<DTS:Variable 
    DTS:ObjectName="BatchSize" 
    DTS:Namespace="User" 
    DTS:DataType="3">
    <DTS:VariableValue>1000</DTS:VariableValue>
</DTS:Variable>

<DTS:Variable 
    DTS:ObjectName="Environment" 
    DTS:Namespace="User" 
    DTS:DataType="8">
    <DTS:VariableValue>production</DTS:VariableValue>
</DTS:Variable>

<DTS:Variable 
    DTS:ObjectName="EnableLogging" 
    DTS:Namespace="User" 
    DTS:DataType="11">
    <DTS:VariableValue>True</DTS:VariableValue>
</DTS:Variable>
```

**Generated PySpark:**
```python
# ── Variables (from DTS:Variable) ──────────────────────────────
batch_size = 1000
environment = "production"
enable_logging = True
```

### Date Variables

**SSIS XML:**
```xml
<DTS:Variable 
    DTS:ObjectName="StartDate" 
    DTS:Namespace="User" 
    DTS:DataType="7">
    <DTS:VariableValue>2024-01-01T00:00:00</DTS:VariableValue>
</DTS:Variable>
```

**Generated PySpark:**
```python
from datetime import datetime

# ── Variables (from DTS:Variable) ──────────────────────────────
start_date = datetime(2024, 1, 1, 0, 0, 0)
```

### Read-Only Constants

**SSIS XML:**
```xml
<DTS:Variable 
    DTS:ObjectName="MaxRetries" 
    DTS:Namespace="User" 
    DTS:DataType="3"
    DTS:ReadOnly="True">
    <DTS:VariableValue>3</DTS:VariableValue>
</DTS:Variable>
```

**Generated PySpark:**
```python
# ── Variables (from DTS:Variable) ──────────────────────────────
MAX_RETRIES = 3  # Read-only constant
```

### Expression Variables (LLM PATCH)

**SSIS XML:**
```xml
<DTS:Variable 
    DTS:ObjectName="OutputPath" 
    DTS:Namespace="User" 
    DTS:DataType="8">
    <DTS:VariableValue 
        DTS:EvaluateAsExpression="True">
        @[User::BasePath] + "\\" + @[User::Environment] + "\\output"
    </DTS:VariableValue>
</DTS:Variable>
```

**Generated PySpark (LLM PATCH):**
```python
# ── Variables (from DTS:Variable) ──────────────────────────────
base_path = "/mnt/data"
environment = "production"

# Expression variable (evaluated at runtime)
output_path = f"{base_path}/{environment}/output"
```

### Decimal Variables

**SSIS XML:**
```xml
<DTS:Variable 
    DTS:ObjectName="TaxRate" 
    DTS:Namespace="User" 
    DTS:DataType="14">
    <DTS:VariableValue>0.0825</DTS:VariableValue>
</DTS:Variable>
```

**Generated PySpark:**
```python
from decimal import Decimal

# ── Variables (from DTS:Variable) ──────────────────────────────
tax_rate = Decimal('0.0825')
```

## Variable Scope

### Package-Level Variables

Variables defined at the package level become module-level Python variables:

**SSIS Structure:**
```
Package
├── Variable: BatchSize
├── Variable: Environment
└── Data Flow Task
```

**Generated PySpark:**
```python
# Module-level variables
batch_size = 1000
environment = "production"

def run():
    # Can access batch_size and environment here
    logger.info(f"Processing with batch size: {batch_size}")
```

### Container-Scoped Variables

Variables inside containers (ForEach, For Loop, Sequence) become local variables:

**SSIS Structure:**
```
Package
└── ForEach Loop Container
    ├── Variable: CurrentFile (scoped to container)
    └── Data Flow Task
```

**Generated PySpark:**
```python
def process_files():
    # Container-scoped variable becomes local
    for current_file in file_list:
        logger.info(f"Processing: {current_file}")
        # ... processing logic
```

## System Variables

Most SSIS System variables are skipped or mapped to Spark context:

| SSIS System Variable | PySpark Equivalent | Notes |
|---|---|---|
| `System::PackageName` | `spark.conf.get("spark.app.name")` | Application name |
| `System::StartTime` | `datetime.now()` | Execution start time |
| `System::MachineName` | `socket.gethostname()` | Host machine |
| `System::UserName` | `os.environ.get("USER")` | Current user |
| `System::TaskName` | String literal | Task identifier |
| `System::ContainerStartTime` | `datetime.now()` | Container start |
| `User::BatchId` | Produced by Batch Lookup | Unique ID for the batch |
| `User::BatchRunId` | Generated per execution | Unique ID for the current run |
| `User::ExtractCount` | `df.count()` from source | Input row count |
| `User::ErrorCount` | `error_df.count()` | Rejected row count |
| `User::LoadCount` | `ExtractCount - ErrorCount` | Successfully loaded row count |

**Example:**
```python
import socket
import os
from datetime import datetime

# System variables (if referenced in SSIS)
package_name = spark.conf.get("spark.app.name", "ETL_Package")
start_time = datetime.now()
machine_name = socket.gethostname()
user_name = os.environ.get("USER", "unknown")

# Audit and Batch Variables
batch_id = 0  # To be updated by lookup_batch
batch_run_id = int(time.time())  # Unique run identifier
extract_count = 0
error_count = 0
load_count = 0
```

## Variable Name Sanitization

SSIS variable names may contain characters invalid in Python. The parser sanitizes them:

| SSIS Variable Name | Sanitized Python Name |
|---|---|
| `User::BatchSize` | `batch_size` |
| `User::File Path` | `file_path` |
| `User::Max-Retries` | `max_retries` |
| `User::2024_Budget` | `budget_2024` |

**Sanitization Rules:**
1. Remove namespace prefix (`User::`, `System::`)
2. Convert to snake_case
3. Replace spaces and hyphens with underscores
4. Move leading digits to end
5. Remove special characters

**Implementation:**
```python
def sanitize_variable_name(ssis_name: str) -> str:
    """Convert SSIS variable name to valid Python identifier."""
    # Remove namespace
    name = ssis_name.split("::")[-1]
    
    # Convert to snake_case
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    
    # Replace invalid characters
    name = re.sub(r'[^a-z0-9_]', '_', name)
    
    # Handle leading digits
    if name[0].isdigit():
        name = f"var_{name}"
    
    return name
```

## Complete Example

**SSIS Package Variables:**
```xml
<DTS:Variables>
    <DTS:Variable DTS:ObjectName="BatchSize" DTS:Namespace="User" DTS:DataType="3">
        <DTS:VariableValue>1000</DTS:VariableValue>
    </DTS:Variable>
    
    <DTS:Variable DTS:ObjectName="StartDate" DTS:Namespace="User" DTS:DataType="7">
        <DTS:VariableValue>2024-01-01T00:00:00</DTS:VariableValue>
    </DTS:Variable>
    
    <DTS:Variable DTS:ObjectName="Environment" DTS:Namespace="User" DTS:DataType="8">
        <DTS:VariableValue>production</DTS:VariableValue>
    </DTS:Variable>
    
    <DTS:Variable DTS:ObjectName="EnableLogging" DTS:Namespace="User" DTS:DataType="11">
        <DTS:VariableValue>True</DTS:VariableValue>
    </DTS:Variable>
    
    <DTS:Variable DTS:ObjectName="MaxRetries" DTS:Namespace="User" DTS:DataType="3" DTS:ReadOnly="True">
        <DTS:VariableValue>3</DTS:VariableValue>
    </DTS:Variable>
</DTS:Variables>
```

**Generated PySpark:**
```python
# AUTO-GENERATED by SSIS-to-PySpark Rule Engine
# Package: HealthcareETL
# Generated: 2026-02-17 14:43:11

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("HealthcareETL") \
    .getOrCreate()

# ── Variables (from DTS:Variable) ──────────────────────────────
batch_size = 1000
start_date = datetime(2024, 1, 1, 0, 0, 0)
environment = "production"
enable_logging = True
MAX_RETRIES = 3  # Read-only constant

# ── Control Flow ───────────────────────────────────────────────
def run():
    logger.info(f"Starting ETL in {environment} environment")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Processing from: {start_date}")
    
    # ... ETL logic using variables
```

## Parser Implementation

The `SSISParser` extracts variables in `_parse_variables()`:

```python
def _parse_variables(self, root):
    variables = []
    for var in root.findall(".//DTS:Variable", self.ns):
        name = var.get(f"{{{self.ns['DTS']}}}ObjectName", "")
        namespace = var.get(f"{{{self.ns['DTS']}}}Namespace", "User")
        datatype = var.get(f"{{{self.ns['DTS']}}}DataType", "8")
        readonly = var.get(f"{{{self.ns['DTS']}}}ReadOnly", "False")
        
        value_elem = var.find("DTS:VariableValue", self.ns)
        value = value_elem.text if value_elem is not None else ""
        
        is_expression = value_elem.get(
            f"{{{self.ns['DTS']}}}EvaluateAsExpression", "False"
        ) == "True" if value_elem is not None else False
        
        variables.append({
            "name": self._sanitize_variable_name(name),
            "namespace": namespace,
            "datatype": datatype,
            "value": value,
            "readonly": readonly == "True",
            "is_expression": is_expression
        })
    
    return variables
```

## References

- **SSIS XML Reference**: `knowledge/ssis_xml_reference.md`
- **Connection Manager Rules**: `knowledge/connection_manager_rules.md`
- **PySpark Skeleton Template**: `knowledge/pyspark_skeleton_template.md`
- **Parser Implementation**: `src/parsers/ssis_dtsx.py`
- **Generator**: `src/generators/pyspark_output.py`
