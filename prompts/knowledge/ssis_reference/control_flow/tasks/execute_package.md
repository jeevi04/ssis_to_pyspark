# SSIS Execute Package Task - Knowledge Base

## Purpose
The Execute Package task extends the capabilities of SSIS by letting a package run other packages as part of a workflow.

## PySpark Equivalent
In terms of modular code, this is equivalent to calling another Python module or script that contains the child package logic.

### Pattern: Sequential Execution
If child packages are run sequentially:
```python
import child_package_module

def execute_package_task():
    # Pass necessary variables/parameters
    child_package_module.run(variables)
```

### Pattern: Orchestration (Databricks)
If running on Databricks, use `dbutils.notebook.run`:
```python
def execute_package_task(notebook_path, params):
    dbutils.notebook.run(notebook_path, 0, params)
```

## Key Considerations
- **Variable Passing**: SSIS uses "Package Configurations" or "Parameters" to pass values. In Python, use function arguments or a shared state dictionary.
- **Transaction Context**: If the parent package has a transaction, ensuring the child package joins it is complex in Python. Usually, atomicity is handled at the data layer (e.g., Delta Lake).
