# Connection Manager Conversion Rules

This document defines how SSIS Connection Managers are converted to PySpark connection configurations.

## Overview

Each `DTS:ConnectionManager` element has a `CreationName` attribute identifying its type. The rule engine maps these to PySpark connection patterns.

## 2.1 OLE DB / ODBC / ADO.NET Connection Managers → JDBC Config

| SSIS XML Attribute / Property | XPath to Value | PySpark Output (RULE) |
|---|---|---|
| **Connection name** | `@DTS:ObjectName` | `conn_name = "{value}"` |
| **CreationName = OLEDB** | `@DTS:CreationName` | Use jdbc format with SQL Server driver |
| **ConnectionString** | `DTS:ObjectData/DTS:ConnectionManager/@DTS:ConnectionString` | Parse into `jdbc_url` + `properties` dict |
| **Server name** (parsed) | `Data Source=<server>` | `jdbc_url = "jdbc:sqlserver://{server}"` |
| **Database name** (parsed) | `Initial Catalog=<db>` | `jdbc_url += ";databaseName={db}"` |
| **Integrated Security=SSPI** | `Integrated Security=SSPI` | `properties['integratedSecurity'] = 'true'` |
| **User ID** (parsed) | `User ID=<user>` | `properties['user'] = '{user}'` |
| **Password** (parsed) | `Password=<pwd>` | `properties['password'] = os.environ['{conn}_PWD']` |
| **Provider=SQLNCLI** | `Provider=` attribute | `driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'` |
| **Provider=OraOLEDB** | `Provider=` attribute | `jdbc_url = "jdbc:oracle:thin:@..."` # LLM PATCH |
| **Provider=MySQLProv** | `Provider=` attribute | `jdbc_url = "jdbc:mysql://..."` # LLM PATCH |
| **Flat File CM** | `DTS:CreationName=FLATFILE` | path variable (see Section 2.2) |
| **Excel CM** | `DTS:CreationName=EXCEL` | path variable + `spark.read.format('com.crealytics.spark.excel')` |
| **FTP CM** | `DTS:CreationName=FTP` | `ftplib` config dict # LLM PATCH |
| **SMTP CM** | `DTS:CreationName=SMTP` | `smtplib` config dict # LLM PATCH |
| **HTTP CM** | `DTS:CreationName=HTTP` | `requests.Session` config # LLM PATCH |

## Connection String Parsing

SSIS connection strings use semicolon-delimited key-value pairs. The parser extracts these into structured configs.

### SQL Server Example

**SSIS ConnectionString:**
```
Data Source=localhost;Initial Catalog=HealthcareDB;Provider=SQLNCLI11;Integrated Security=SSPI;
```

**Parsed PySpark Config:**
```python
JDBC_CONFIG_HealthcareDB = {
    "url": "jdbc:sqlserver://localhost;databaseName=HealthcareDB",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "properties": {
        "integratedSecurity": "true"
    }
}
```

### SQL Server with Username/Password

**SSIS ConnectionString:**
```
Data Source=prod-server;Initial Catalog=SalesDB;User ID=etl_user;Password=***;Provider=SQLNCLI11;
```

**Parsed PySpark Config:**
```python
import os

JDBC_CONFIG_SalesDB = {
    "url": "jdbc:sqlserver://prod-server;databaseName=SalesDB",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "properties": {
        "user": "etl_user",
        "password": os.environ.get("SALESDB_PWD", "")  # Security: use env var
    }
}
```

## 2.2 Flat File Connection Managers → File Paths

### Property Mapping Table

| SSIS XML Property | XML Path | PySpark Output (RULE) |
|---|---|---|
| **File path** | `@DTS:ConnectionString` | `flat_file_path = '{value}'` |
| **Format = Delimited** | `DTS:Property[Name='Format']='Delimited'` | `spark.read.csv(...)` |
| **Format = FixedWidth** | `DTS:Property[Name='Format']='FixedWidth'` | `spark.read.text(...)` # LLM PATCH width parsing |
| **Column Delimiter** | `DTS:Property[Name='ColumnDelimiter']` | `sep='{value}'` |
| **Row Delimiter = {CR}{LF}** | `DTS:Property[Name='RowDelimiter']` | `lineSep='\r\n'` (default) |
| **Header Row exists** | `DTS:Property[Name='HeaderRowsToSkip']` | `header='true'` |
| **Unicode flag** | `DTS:Property[Name='Unicode']='True'` | `encoding='utf-8'` |
| **Code Page** | `DTS:Property[Name='CodePage']` | `encoding=codecs.lookup(cp).name` |
| **TextQualifier** | `DTS:Property[Name='TextQualifier']` | `quote='{value}'` |
| **ColumnDefinition names** | `DTS:FlatFileColumn@DTS:ObjectName` | schema StructField names |
| **ColumnDefinition types** | `DTS:FlatFileColumn DTS:DataType` | Mapped via data type table (Section 7) |
| **ColumnDefinition width** | `DTS:FlatFileColumn DTS:ColumnWidth` | StringType + trim for fixed-width |

### Delimited File Example

**SSIS ConnectionString:**
```
C:\Data\Patients.csv
```

**SSIS Properties:**
- Format: Delimited
- Column Delimiter: `,`
- Text Qualifier: `"`
- Header Rows: 1
- Unicode: True
- Code Page: 65001 (UTF-8)

**Parsed PySpark Config:**
```python
FILE_PATH_Patients = "/mnt/data/Patients.csv"

# Read operation
df_patients = spark.read.csv(
    FILE_PATH_Patients,
    header=True,
    inferSchema=True,
    quote='"',
    sep=',',
    encoding='utf-8'
)
```

### Fixed-Width File Example

**SSIS Properties:**
- Format: FixedWidth
- Column Definitions:
  - PatientID: width 10
  - FirstName: width 20
  - LastName: width 20
  - BirthDate: width 10

**Parsed PySpark Config (LLM PATCH):**
```python
FILE_PATH_FixedWidth = "/mnt/data/patients_fixed.txt"

# Read as text and parse fixed-width columns
df_raw = spark.read.text(FILE_PATH_FixedWidth)

df_patients = df_raw.select(
    F.trim(F.substring(F.col("value"), 1, 10)).alias("PatientID"),
    F.trim(F.substring(F.col("value"), 11, 20)).alias("FirstName"),
    F.trim(F.substring(F.col("value"), 31, 20)).alias("LastName"),
    F.trim(F.substring(F.col("value"), 51, 10)).alias("BirthDate")
)
```

### Code Page Mapping

Common SSIS code pages and their Python equivalents:

| Code Page | Description | Python Encoding |
|---|---|---|
| 1252 | Windows Latin-1 | `windows-1252` |
| 65001 | UTF-8 | `utf-8` |
| 1200 | UTF-16 LE | `utf-16-le` |
| 1201 | UTF-16 BE | `utf-16-be` |
| 28591 | ISO 8859-1 | `iso-8859-1` |
| 932 | Japanese Shift-JIS | `shift-jis` |

**Implementation:**
```python
import codecs

def get_encoding_from_codepage(code_page: int) -> str:
    """Convert SSIS code page to Python encoding name."""
    try:
        encoding_info = codecs.lookup(code_page)
        return encoding_info.name
    except LookupError:
        return 'utf-8'  # Default fallback
```


## 2.3 Excel Connection Managers → Excel Reader

| SSIS XML Attribute / Property | XPath to Value | PySpark Output (RULE) |
|---|---|---|
| **Connection name** | `@DTS:ObjectName` | `EXCEL_PATH_{name}` |
| **ConnectionString** | `DTS:ObjectData/DTS:ConnectionManager/@DTS:ConnectionString` | File path string |
| **Excel Version** | Parsed from connection string | Format parameter |

### Excel Example

**SSIS ConnectionString:**
```
Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\Data\Sales.xlsx;Extended Properties="Excel 12.0 XML;HDR=YES";
```

**Parsed PySpark Config:**
```python
EXCEL_PATH_Sales = "/mnt/data/Sales.xlsx"

# Read operation (requires spark-excel library)
df_sales = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Sheet1'!A1") \
    .load(EXCEL_PATH_Sales)
```

## 2.4 Oracle Connection Managers → Oracle JDBC

**SSIS ConnectionString:**
```
Data Source=ORCL;User ID=hr;Password=***;Provider=OraOLEDB.Oracle;
```

**Parsed PySpark Config:**
```python
import os

JDBC_CONFIG_Oracle = {
    "url": "jdbc:oracle:thin:@localhost:1521:ORCL",
    "driver": "oracle.jdbc.driver.OracleDriver",
    "properties": {
        "user": "hr",
        "password": os.environ.get("ORACLE_PWD", "")
    }
}
```

## 2.5 MySQL Connection Managers → MySQL JDBC

**SSIS ConnectionString:**
```
Server=mysql-server;Database=ecommerce;Uid=app_user;Pwd=***;Provider=MySQLProv;
```

**Parsed PySpark Config:**
```python
import os

JDBC_CONFIG_MySQL = {
    "url": "jdbc:mysql://mysql-server:3306/ecommerce",
    "driver": "com.mysql.cj.jdbc.Driver",
    "properties": {
        "user": "app_user",
        "password": os.environ.get("MYSQL_PWD", "")
    }
}
```

## 2.6 FTP Connection Managers → FTP Config

**SSIS Properties:**
- Server: `ftp.example.com`
- Port: `21`
- User: `ftpuser`
- Password: `***`

**Parsed PySpark Config:**
```python
import os
from ftplib import FTP

FTP_CONFIG_DataFeed = {
    "host": "ftp.example.com",
    "port": 21,
    "user": "ftpuser",
    "password": os.environ.get("FTP_PWD", "")
}

# Usage (LLM PATCH - custom implementation needed)
def download_from_ftp(config, remote_path, local_path):
    with FTP(config["host"]) as ftp:
        ftp.login(config["user"], config["password"])
        with open(local_path, 'wb') as f:
            ftp.retrbinary(f'RETR {remote_path}', f.write)
```

## 2.7 HTTP Connection Managers → Requests Config

**SSIS Properties:**
- Server URL: `https://api.example.com`
- Timeout: `30`

**Parsed PySpark Config:**
```python
import requests

HTTP_CONFIG_API = {
    "base_url": "https://api.example.com",
    "timeout": 30,
    "headers": {
        "Content-Type": "application/json"
    }
}

# Usage (LLM PATCH - custom implementation needed)
def fetch_from_api(config, endpoint):
    response = requests.get(
        f"{config['base_url']}/{endpoint}",
        timeout=config['timeout'],
        headers=config['headers']
    )
    return response.json()
```

## 2.8 Multiple OLE DB Connection Managers → Multi-CM Flat Config Dict

When an SSIS package contains **more than one OLE DB / ODBC Connection Manager** (e.g.
SourceDB + TargetDB + NPILookupDB), generate a **single flat dict** with one named URL key
per Connection Manager. Do NOT generate separate config dicts or a plain `{"url": ...}` dict.

**SSIS package has 3 CMs:** `SourceDB`, `TargetDB`, `NPILookupDB`

```python
import os

def build_pipeline_jdbc_config() -> dict:
    """
    Single flat JDBC config for all Connection Managers in the package.
    Named URL keys let Bronze, Silver, and main.py route each query to the
    correct database without rebuilding the config.
    """
    def _sqlserver_url(host, port, db):
        return (f"jdbc:sqlserver://{host}:{port};"
                f"databaseName={db};encrypt=true;trustServerCertificate=true")

    return {
        # One named key per Connection Manager
        "source_url": _sqlserver_url(
            os.environ.get("SOURCE_DB_HOST", "source-server"),
            os.environ.get("SOURCE_DB_PORT", "1433"),
            os.environ.get("SOURCE_DB_NAME", "SourceDB"),
        ),
        "target_url": _sqlserver_url(
            os.environ.get("TARGET_DB_HOST", "target-server"),
            os.environ.get("TARGET_DB_PORT", "1433"),
            os.environ.get("TARGET_DB_NAME", "TargetDB"),
        ),
        "npi_url": _sqlserver_url(
            os.environ.get("NPI_DB_HOST", "npi-server"),
            os.environ.get("NPI_DB_PORT", "1433"),
            # RULE: NPILookupDB always defaults to 'NPIRegistry' catalog
            os.environ.get("NPI_DB_NAME", "NPIRegistry"),
        ),
        # Shared service-account credentials (change to per-CM if accounts differ)
        "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user":     os.environ.get("ETL_DB_USER", ""),
        "password": os.environ.get("ETL_DB_PASSWORD", ""),
    }

# Downstream usage — derive a single-URL config by spreading + overriding 'url':
# source_cfg = {**jdbc_config, "url": jdbc_config["source_url"]}
# target_cfg = {**jdbc_config, "url": jdbc_config["target_url"]}
# npi_cfg    = {**jdbc_config, "url": jdbc_config["npi_url"]}
# Then pass source_cfg / target_cfg / npi_cfg to generic_extract() or spark.read.format("jdbc").
```

**Rules for Multi-CM packages:**
- Always use `build_pipeline_jdbc_config()` — not `get_jdbc_config()` — when the package has >1 CM.
- Key naming: `<logical_cm_name>_url` (e.g. `source_url`, `target_url`, `npi_url`).
- Silver Lookup functions must accept the full `jdbc_config` dict and pick the correct key
  matching the SSIS Lookup component's Connection Manager (see `silver_layer_patterns.md` Rule S-LKP-1).
- `main.py` must **import** `build_pipeline_jdbc_config` from the Bronze module — it must
  NOT define a local copy (see `main_orchestrator_patterns.md` Rule M-2).

---

## Implementation Notes

### Security Best Practices

1. **Never hardcode passwords** — always use environment variables
2. **Use Azure Key Vault / AWS Secrets Manager** in production
3. **Rotate credentials regularly**
4. **Use managed identities** when possible (Azure SQL, Databricks)

### LLM PATCH Markers

Some connection types require custom implementation beyond simple JDBC:
- **FTP/SFTP**: Use `ftplib` or `paramiko`
- **HTTP/REST**: Use `requests` library
- **SMTP**: Use `smtplib` for email notifications
- **Oracle/MySQL**: May need vendor-specific JDBC drivers

The LLM generator should recognize these and provide appropriate Python implementations.

## Parser Implementation

The `SSISParser` extracts connection managers in `_parse_connection_managers()`:

```python
def _parse_connection_managers(self, root):
    conn_managers = []
    for cm in root.findall(".//DTS:ConnectionManager", self.ns):
        name = cm.get(f"{{{self.ns['DTS']}}}ObjectName", "")
        creation_name = cm.get(f"{{{self.ns['DTS']}}}CreationName", "")
        conn_string = cm.get(f"{{{self.ns['DTS']}}}ConnectionString", "")
        
        conn_managers.append({
            "name": name,
            "type": creation_name,
            "connection_string": conn_string
        })
    
    return conn_managers
```

## References

- **SSIS XML Reference**: `knowledge/ssis_xml_reference.md`
- **PySpark Skeleton Template**: `knowledge/pyspark_skeleton_template.md`
- **Parser Implementation**: `src/parsers/ssis_dtsx.py`
- **Generator**: `src/generators/pyspark_output.py`
