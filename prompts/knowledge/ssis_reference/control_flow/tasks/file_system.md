# SSIS File System Task - Knowledge Base

## Purpose
Performs operations on files and directories (Copy, Move, Delete, Create, etc.).

## PySpark Equivalent
Use standard Python `os` or `shutil` modules, or `dbutils.fs` if on Databricks.

### Patterns

#### 1. Copy/Move File
```python
import shutil

def file_system_task_copy(source, destination):
    shutil.copy2(source, destination)

def file_system_task_move(source, destination):
    shutil.move(source, destination)
```

#### 2. Delete File/Directory
```python
import os
import shutil

def file_system_task_delete(path):
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
```

#### 3. Create Directory
```python
import os

def file_system_task_mkdir(path):
    os.makedirs(path, exist_ok=True)
```

## Key Considerations
- **Storage Type**: If working with HDFS or S3, use appropriate APIs (e.g., `pyarrow` for HDFS, `boto3` for S3).
- **Variable Mapping**: SSIS uses `IsSourcePathVariable` and `IsDestinationPathVariable`. In Python, consistently use variables passed to the function.
