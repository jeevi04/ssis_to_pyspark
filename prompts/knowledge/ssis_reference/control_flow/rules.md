# Control Flow Conversion Rules

This document defines how SSIS Control Flow elements are converted to Python control flow in PySpark code.

## Overview

The rule engine reads all `DTS:PrecedenceConstraint` elements and reconstructs the execution graph, then maps it to Python control flow structures.

## 4.1 Precedence Constraints → Python Control Flow

| SSIS Constraint Value | SSIS EvalOp | Python Output (RULE) |
|---|---|---|
| **Success (0)** | Constraint only | sequential call (no guard) |
| **Failure (1)** | Constraint only | placed inside `except` block |
| **Completion (2)** | Constraint only | placed inside `finally` block |
| **Success + Expression** | ExpressionAndConstraint | `if {expr}:` # LLM PATCH expression |
| **Failure + Expression** | ExpressionAndConstraint | `except Exception:\n  if {expr}:` # LLM PATCH |
| **Expression only** | Expression | `if {expr}:` # LLM PATCH |
| **LogicalAnd (multiple)** | `@DTS:LogicalAnd='True'` | all predecessor results must pass |
| **LogicalOr (multiple)** | LogicalAnd absent | any predecessor result passes |
- **[Logging](#logging-integration)**: Using standard Python logging.
- **[Error Handling](#standard-error-handling)**: try-except-finally patterns.
- **[Medallion Layering](#medallion-layering-logic)**: Cross-layer table discovery.

## Medallion Layering Logic

When converting complex SSIS packages to Medallion Architecture (Bronze → Silver → Gold), follow these rules for table discovery:

1. **Bronze Layer**: Produces raw tables named after the source system or entity (e.g., `bronze.address`).
2. **Silver Layer (Read)**: Should read from the corresponding Bronze tables. If an SSIS Data Flow references an ODS or Staging table that wasn't produced by the current Bronze script, it should fallback to the primary entity table in Bronze or handle the absence gracefully.
3. **Silver Layer (Write)**: Produces cleansed tables (e.g., `silver.batches`, `silver.merged`).
4. **Column Mapping**: Silver layer transformations MUST use the actual column names extracted from the SSIS identifiers (e.g., `AddressIdentifier`) rather than generic placeholders (e.g., `entity_id`).
5. **Naming Discoverability**: To prevent mismatches in the Gold layer's `tables_to_promote` list, Silver layer output table names MUST be standardized or documented. The transformation logic should prioritize using the Data Flow Task's sanitised name (e.g., `batches`, `merged`) as the table name in the `silver` database.

---

## Precedence Constraints
 Examples

**Success Constraint (Sequential):**
```python
# Task A → Task B (on success)
task_a()
task_b()  # Runs after task_a completes successfully
```

**Failure Constraint:**
```python
# Task A → Task B (on failure)
try:
    task_a()
except Exception as e:
    logger.error(f"Task A failed: {e}")
    task_b()  # Runs only if task_a fails
```

**Completion Constraint:**
```python
# Task A → Task B (on completion)
try:
    task_a()
finally:
    task_b()  # Runs regardless of task_a success/failure
```

**Expression Constraint (LLM PATCH):**
```python
# Task A → Task B (if variable > 100)
task_a()
if batch_size > 100:  # Expression evaluated
    task_b()
```

## 4.2 Container Rules

| SSIS Container Type | XML CreationName | Python Output (RULE) |
|---|---|---|
| **Sequence Container** | `SSIS.Sequence.3` | `def {name}():` (function wrapping children) |
| **For Loop Container** | `SSIS.ForLoop.3` | `for` loop using InitExpression/EvalExpression/AssignExpression |
| **Foreach Loop – File** | `SSIS.ForEachLoop.3` + FileEnumerator | `for file in glob.glob(pattern):` |
| **Foreach Loop – ADO** | `SSIS.ForEachLoop.3` + ADOEnumerator | `for row in df.collect():` |
| **Foreach Loop – Item** | `SSIS.ForEachLoop.3` + ItemEnumerator | `for item in [{items}]:` |
| **Foreach Loop – Variable** | `SSIS.ForEachLoop.3` + FromVariableEnumerator | `for item in {variable}:` |
| **Foreach Loop – NodeList** | `SSIS.ForEachLoop.3` + NodeListEnumerator | `for node in tree.findall('{xpath}'):` # LLM PATCH |
| **Foreach Loop – SMO** | `SSIS.ForEachLoop.3` + SMOEnumerator | # LLM PATCH – SMO enumeration |

### 4.2.1 For Loop Extracted Properties

| SSIS Property | XML Path | Python Mapping (RULE) |
|---|---|---|
| **InitExpression** | `DTS:Property[Name='InitExpression']` | Python assignment before loop: `{expr}` |
| **EvalExpression** | `DTS:Property[Name='EvalExpression']` | `while` condition (LLM evaluates expression) |
| **AssignExpression** | `DTS:Property[Name='AssignExpression']` | Loop increment statement |
| **VariableMappings** | `DTS:ForEachVariableMapping` | Loop variable = assign collected value |

### Container Examples

**Sequence Container:**
```python
def process_customer_data():
    """Sequence Container: Customer Processing"""
    extract_customers()
    transform_customers()
    load_customers()

# Called from main control flow
process_customer_data()
```

**For Loop Container:**
```python
# InitExpression: @Counter = 1
# EvalExpression: @Counter <= 10
# AssignExpression: @Counter = @Counter + 1

counter = 1
while counter <= 10:
    process_batch(counter)
    counter = counter + 1
```

**Foreach Loop – File Enumerator:**
```python
import glob

# Foreach file in folder
for file_path in glob.glob("/mnt/data/input/*.csv"):
    current_file = file_path
    logger.info(f"Processing file: {current_file}")
    process_file(current_file)
```

**Foreach Loop – ADO Enumerator:**
```python
# Foreach row in result set
df_files = spark.sql("SELECT file_path FROM file_registry WHERE status = 'pending'")
for row in df_files.collect():
    current_file = row['file_path']
    process_file(current_file)
```

## 4.3 Task Rules

### 4.3.1 Data Flow Task

A Data Flow Task (`SSIS.Pipeline.3`) becomes a dedicated Python function. Its contents are handled entirely in Section 5.

| SSIS Property | XML Path | Python Output (RULE) |
|---|---|---|
| **Task Name** | `@DTS:ObjectName` | `def transform_{prefix}_{sanitised_name}(spark):` |
| **Prefixes** | — | `dft` (Data Flow), `sql` (SQL Task), `scr` (Script), `etc` |
| **Description** | `@DTS:Description` | `# {description}` |
| **Disabled=True** | `@DTS:Disabled='True'` | `# DISABLED – function defined but not called` |
| **TransactionOption** | `@DTS:TransactionOption` | `# Transaction scope noted as comment` |

**Naming Rule**: Always use the prefix matching the component type followed by the object name (e.g., `transform_dft_generatebatches`). This is critical for automated testing and discovery.

**Example:**
```python
def data_flow_load_customers(spark):
    """Load and transform customer data"""
    # Data flow implementation (see Section 5)
    df_source = spark.read.jdbc(...)
    df_transformed = df_source.withColumn(...)
    df_transformed.write.jdbc(...)
```

### 4.3.2 Execute SQL Task

| SSIS Property | XML Path | Python Output (RULE) |
|---|---|---|
| **Task Name** | `@DTS:ObjectName` | `def {name}():` |
| **Connection** | `SQLTask:SqlTaskData@SQLTask:Connection` | `conn = get_connection('{conn_name}')` |
| **SQLStatement** | `SQLTask:SqlTaskData@SQLTask:SqlStatementSource` | `spark.sql('''{sql}''')` or `jdbc.execute(sql)` |
| **ResultSetType = None** | `@SQLTask:ResultSet='None'` | `spark.sql(sql)` (no result captured) |
| **ResultSetType = SingleRow** | `@SQLTask:ResultSet='SingleRow'` | `result = spark.sql(sql).first()` |
| **ResultSetType = Full** | `@SQLTask:ResultSet='Full'` | `df = spark.sql(sql)` |
| **ResultSetType = XML** | `@SQLTask:ResultSet='Xml'` | `xml_str = spark.sql(sql).first()[0]` |
| **ResultSetBinding** | `SQLTask:ResultSetBinding` | variable assignment: `{var} = result[{col}]` |
| **ParameterMappings** | `SQLTask:ParameterBinding` | Parameterized query with `.format()` or f-string |
| **IsQueryStoredProc** | `@SQLTask:IsStoredProcedure='True'` | `CALL {proc}({params})` # LLM PATCH params |
| **TimeOut** | `@SQLTask:TimeOut` | `spark.conf.set('spark.sql.queryExecutionListeners', ...)` |
| **CodePage** | `@SQLTask:CodePage` | `# encoding hint` (SKIP for Spark SQL) |

**Example:**
```python
def execute_sql_truncate_staging():
    """Truncate staging tables"""
    sql = """
    TRUNCATE TABLE staging.customers;
    TRUNCATE TABLE staging.orders;
    """
    spark.sql(sql)
    logger.info("Staging tables truncated")

def execute_sql_get_max_id():
    """Get maximum customer ID"""
    sql = "SELECT MAX(customer_id) as max_id FROM customers"
    result = spark.sql(sql).first()
    max_customer_id = result['max_id'] if result else 0
    return max_customer_id
```

### 4.3.3 Execute Package Task

| SSIS Property | XML Path | Python Output (RULE) |
|---|---|---|
| **PackageName (project)** | `EptTaskData@PackageName` | `import {module}; {module}.run(spark)` |
| **PackageName (file path)** | `EptTaskData@PackagePath` | `subprocess.run(['python', '{path}.py'])` # LLM PATCH |
| **Parameter mappings** | `EptTaskData:Parameters` | Pass as function arguments # LLM PATCH |
| **ExecuteOutOfProcess** | `@ExecuteOutOfProcess='True'` | `subprocess.run([...])` |

**Example:**
```python
# Execute package from project
import child_package
child_package.run(spark, batch_size=1000, environment="production")

# Execute package from file (out of process)
import subprocess
result = subprocess.run(
    ['python', '/mnt/scripts/legacy_etl.py'],
    capture_output=True,
    text=True
)
if result.returncode != 0:
    raise Exception(f"Child package failed: {result.stderr}")
```

### 4.3.4 Script Task

Script Tasks have a binary-encoded ScriptProject embedded in the XML. The rule engine extracts the metadata; the LLM reconstitutes the logic.

| SSIS Property | XML Path | Output (RULE / LLM) |
|---|---|---|
| **Task name** | `@DTS:ObjectName` | `def {name}(spark, variables):` # scaffold |
| **ScriptLanguage** | `ScriptTask:ScriptTaskData@ScriptTask:ScriptLanguage` | `# Source language: {lang}` |
| **ReadOnlyVariables** | `@ScriptTask:ReadOnlyVariables` | `# Input vars: {list}` – pass as params |
| **ReadWriteVariables** | `@ScriptTask:ReadWriteVariables` | `# Output vars: {list}` – return as dict |
| **EntryPoint** | `@ScriptTask:EntryPoint` | `# Entry method: {name}` |
| **ScriptProject (binary)** | CDATA / base64 blob | # LLM PATCH – reconstruct from description |

**Example:**
```python
def script_task_validate_files(spark, variables):
    """
    Script Task: Validate File Counts
    Source language: C#
    Input vars: file_path, expected_count
    Output vars: actual_count, is_valid
    """
    # LLM PATCH – reconstructed logic
    import glob
    
    file_path = variables['file_path']
    expected_count = variables['expected_count']
    
    files = glob.glob(file_path)
    actual_count = len(files)
    is_valid = (actual_count == expected_count)
    
    return {
        'actual_count': actual_count,
        'is_valid': is_valid
    }
```

### 4.3.5 File System Task

| Operation Type | XML OperationType value | Python Output (RULE) |
|---|---|---|
| **Copy File** | 0 | `shutil.copy(src, dst)` |
| **Create Directory** | 1 | `os.makedirs(path, exist_ok=True)` |
| **Delete Directory** | 2 | `shutil.rmtree(path)` |
| **Delete Directory Contents** | 3 | `for f in os.listdir(path): os.remove(...)` |
| **Delete File** | 4 | `os.remove(path)` |
| **Delete Files** | 5 | `for f in glob.glob(pattern): os.remove(f)` |
| **Move Directory** | 6 | `shutil.move(src, dst)` |
| **Move File** | 7 | `shutil.move(src, dst)` |
| **Move Files** | 8 | `for f in glob.glob(pattern): shutil.move(f, dst)` |
| **Rename File** | 9 | `os.rename(src, dst)` |
| **Set Attributes** | 10 | `os.chmod(path, mode)` # LLM PATCH |

| SSIS Property | XML Path | Python Output (RULE) |
|---|---|---|
| **Source path** | `FSTask:FileSystemData@Source` | `src = variables['{var}']` or `'{literal}'` |
| **Destination path** | `FSTask:FileSystemData@Destination` | `dst = variables['{var}']` or `'{literal}'` |
| **OverwriteDestination** | `@OverwriteDestination='True'` | `if os.path.exists(dst): os.remove(dst)` |

**Example:**
```python
import shutil
import os

def file_system_archive_files():
    """Move processed files to archive"""
    src = "/mnt/data/processed"
    dst = "/mnt/data/archive"
    
    # Create archive directory if needed
    os.makedirs(dst, exist_ok=True)
    
    # Move all CSV files
    import glob
    for file_path in glob.glob(f"{src}/*.csv"):
        shutil.move(file_path, dst)
    
    logger.info(f"Files archived to {dst}")
```

### 4.3.6 Send Mail Task

| SSIS Property | XML Path | Python Output (RULE) |
|---|---|---|
| **To** | `MailTaskData@To` | `msg['To'] = '{value}'` |
| **CC** | `MailTaskData@CC` | `msg['Cc'] = '{value}'` |
| **From** | `MailTaskData@From` | `msg['From'] = '{value}'` |
| **Subject** | `MailTaskData@Subject` | `msg['Subject'] = '{value}'` |
| **MessageSource (literal)** | `MailTaskData@MessageSource` | `msg.set_payload('{value}')` |
| **MessageSourceType=Variable** | `@MessageSourceType='Variable'` | `msg.set_payload(str({variable}))` |
| **Priority** | `MailTaskData@Priority` | `msg['X-Priority'] = '{map}'` |
| **SMTPConn reference** | `MailTaskData@SMTPConn` | `smtp = smtplib.SMTP(configs['{conn}']['host'])` |
| **FileAttachments** | `MailTaskData@FileAttachments` | `msg.attach(MIMEBase(...))` # LLM PATCH |

**Example:**
```python
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_mail_completion_notice():
    """Send ETL completion email"""
    msg = MIMEMultipart()
    msg['From'] = 'etl@company.com'
    msg['To'] = 'data-team@company.com'
    msg['Subject'] = 'ETL Process Completed Successfully'
    
    body = f"""
    ETL Process: HealthcareETL
    Status: Completed
    Records Processed: {records_processed}
    Duration: {duration} seconds
    """
    msg.attach(MIMEText(body, 'plain'))
    
    smtp = smtplib.SMTP(SMTP_CONFIG['host'], SMTP_CONFIG['port'])
    smtp.send_message(msg)
    smtp.quit()
```

### 4.3.7 FTP Task

| SSIS Property | XML Path | Python Output (RULE) |
|---|---|---|
| **Operation = Send** | `FtpTask:FtpTaskData@FtpTask:Operation='Send'` | `ftp.storbinary('STOR {remote}', open(local))` |
| **Operation = Receive** | `@Operation='Receive'` | `ftp.retrbinary('RETR {remote}', open(local,'wb').write)` |
| **Operation = CreateDirectory** | `@Operation='CreateDirectory'` | `ftp.mkd('{dir}')` |
| **Operation = RemoveDirectory** | `@Operation='RemoveDirectory'` | `ftp.rmd('{dir}')` |
| **Operation = DeleteFiles** | `@Operation='DeleteFiles'` | `ftp.delete('{file}')` |
| **LocalPath** | `@LocalPath` | `local = '{path_or_variable}'` |
| **RemotePath** | `@RemotePath` | `remote = '{path_or_variable}'` |
| **Connection reference** | `@FtpTask:Connection` | `ftp = ftplib.FTP(configs['{conn}']['host'])` |
| **IsTransferTypeASCII** | `@IsTransferTypeASCII='True'` | `ftp.retrlines(...)` else `ftp.retrbinary(...)` |

**Example:**
```python
from ftplib import FTP

def ftp_download_files():
    """Download files from FTP server"""
    ftp = FTP(FTP_CONFIG['host'])
    ftp.login(FTP_CONFIG['user'], FTP_CONFIG['password'])
    
    remote_path = '/data/input/customers.csv'
    local_path = '/mnt/data/input/customers.csv'
    
    with open(local_path, 'wb') as f:
        ftp.retrbinary(f'RETR {remote_path}', f.write)
    
    ftp.quit()
    logger.info(f"Downloaded {remote_path} to {local_path}")
```

### 4.3.8 Execute Process Task

| SSIS Property | XML Path | Python Output (RULE) |
|---|---|---|
| **Executable** | `ExecProcTaskData@Executable` | `cmd = ['{value}']` |
| **Arguments** | `ExecProcTaskData@Arguments` | `cmd += shlex.split('{value}')` |
| **WorkingDirectory** | `ExecProcTaskData@WorkingDirectory` | `cwd = '{value}'` |
| **StandardInputVariable** | `@StandardInputVariable` | `stdin=subprocess.PIPE` # LLM PATCH |
| **StandardOutputVariable** | `@StandardOutputVariable` | `stdout = result.stdout` |
| **StandardErrorVariable** | `@StandardErrorVariable` | `stderr = result.stderr` |
| **SuccessReturnCode** | `@SuccessReturnCode` | `assert result.returncode == {value}` |
| **TimeOut** | `@TimeOut` | `timeout={value}` |
| **RequireFullFileName** | `@RequireFullFileName` | `shutil.which(cmd[0])` check |

**Example:**
```python
import subprocess
import shlex

def execute_process_data_validation():
    """Run external data validation script"""
    cmd = ['python', '/opt/scripts/validate_data.py']
    cmd += shlex.split('--input /mnt/data/input --threshold 0.95')
    
    result = subprocess.run(
        cmd,
        cwd='/opt/scripts',
        capture_output=True,
        text=True,
        timeout=300
    )
    
    if result.returncode != 0:
        raise Exception(f"Validation failed: {result.stderr}")
    
    validation_output = result.stdout
    logger.info(f"Validation output: {validation_output}")
```

### 4.3.9 Bulk Insert Task

| SSIS Property | XML Path | Python Output (RULE) |
|---|---|---|
| **DestinationTable** | `BulkInsertTask@DestinationTable` | `target_table = '{schema}.{table}'` |
| **SourceConnectionName** | `@SourceConnection` | `src_path = configs['{conn}']['path']` |
| **FieldTerminator** | `@FieldTerminator` | `sep='{value}'` |
| **RowTerminator** | `@RowTerminator` | `lineSep='{value}'` |
| **FirstRow** | `@FirstRow` | `skip = {value}` (skipRows in JDBC write) |
| **LastRow** | `@LastRow` | `limit = {value}` |
| **MaxErrors** | `@MaxErrors` | `# informational comment` |
| **CheckConstraints** | `@CheckConstraints` | `# constraint check note` |
| **FireTriggers** | `@FireTriggers` | `# trigger note` (use JDBC batchsize) |
| **CodePage** | `@CodePage` | `encoding='{cp}'` |
| **Full pattern output** | — | `df.write.jdbc(url, target_table, mode='append', properties=props)` |

**Example:**
```python
def bulk_insert_customers():
    """Bulk insert customer data from CSV"""
    src_path = FILE_PATH_Customers
    target_table = "dbo.Customers"
    
    # Read CSV with bulk insert settings
    df = spark.read.csv(
        src_path,
        sep='|',
        header=False,
        encoding='utf-8'
    )
    
    # Write to target table
    df.write.jdbc(
        url=JDBC_CONFIG['url'],
        table=target_table,
        mode='append',
        properties={
            'driver': JDBC_CONFIG['driver'],
            'batchsize': '10000',
            'isolationLevel': 'READ_UNCOMMITTED'
        }
    )
    
    logger.info(f"Bulk inserted {df.count()} rows into {target_table}")
```

## References

- **SSIS XML Reference**: `knowledge/ssis_xml_reference.md`
- **Connection Manager Rules**: `knowledge/connection_manager_rules.md`
- **Variable Rules**: `knowledge/variable_rules.md`
- **PySpark Skeleton Template**: `knowledge/pyspark_skeleton_template.md`
- **Parser Implementation**: `src/parsers/ssis_dtsx.py`
- **Generator**: `src/generators/pyspark_output.py`

---

## Logging Integration
Map SSIS Logging (SQL Server, Text File, etc.) to the standard Python `logging` module.

```python
import logging
import sys

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("package_log.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SSIS_Converted_Package")

def task_with_logging():
    logger.info("Task Started")
    # task logic
    logger.info("Task Completed Successfully")
```

---

## Standard Error Handling
Use a robust error handling pattern to mimic SSIS `MaximumErrorCount` and `FailPackageOnFailure`.

```python
def robust_execution_pattern(task_func, *args, **kwargs):
    try:
        return task_func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Task Failed: {str(e)}")
        # If FailPackageOnFailure is true:
        sys.exit(1) 
        # If not, return a failure state for precedence check
        return False
```
