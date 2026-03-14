# SSIS Expression Task - Knowledge Base

This document provides conversion patterns for the SSIS Expression Task.

## Purpose
The Expression Task evaluates an SSIS expression and typically assigns the result to a variable. It is a lightweight task commonly used to update counters, timestamps, or flags.

## PySpark Equivalent
In Python, this is a simple assignment statement or a function call.

### Pattern: Variable Assignment
SSIS: `Expression: @User::Counter = @User::Counter + 1`
Python:
```python
# Direct variable update
variables['Counter'] += 1
```

### Pattern: Timestamp Logic
SSIS: `Expression: @User::LastRunTime = GETDATE()`
Python:
```python
from datetime import datetime
variables['LastRunTime'] = datetime.now()
```

### Pattern: String Manipulation
SSIS: `Expression: @User::FilePath = "C:\\Data\\" + @User::FileName`
Python:
```python
variables['FilePath'] = f"C:/Data/{variables['FileName']}"
```

## Key Considerations
- **Scope**: Ensure the variable being updated is within the correct scope (passed to the function by reference or returned in a dictionary).
- **Data Types**: Python is dynamically typed, but SSIS is strict. Ensure the expression result matches the expected type in downstream tasks.
