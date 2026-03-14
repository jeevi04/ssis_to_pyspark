# SSIS Script Task - Knowledge Base

## Purpose
Provides a way to perform custom functions that are not available in the built-in SSIS tasks (using C# or VB.NET).

## PySpark Equivalent
Direct Python code implemented within a function.

### Pattern
```python
def script_task_conversion(variables):
    """
    Equivalent to SSIS Script Task logic.
    Read variables: variables['MyVar']
    Write variables: variables['ResultVar'] = ...
    """
    # Implement custom logic here
    # Example: complex file format parsing, API interaction, etc.
    return variables
```

## Key Considerations
- **Variable Access**: SSIS `ReadOnlyVariables` and `ReadWriteVariables` must be explicitly handled. Map these to keys in a Python dictionary passed to the function.
- **External Libraries**: If the Script Task used .NET libraries, find equivalent Python packages (e.g., `requests` for HTTP, `pandas` for data munging).
- **Error Handling**: Use `try-except` blocks to mimic the `Dts.Events` error reporting in SSIS.
