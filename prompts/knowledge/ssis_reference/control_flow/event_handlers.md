# SSIS Event Handlers - Knowledge Base

## Purpose
Event handlers run in response to events raised by a package, task, or container. Common events include `OnError`, `OnPreExecute`, `OnPostExecute`, and `OnVariableValueChanged`.

## PySpark Equivalent
In Python, event handlers are typically implemented using decorators, context managers, or explicit `try-except-finally` blocks.

### Pattern: OnError (Global Exception Handling)
```python
import logging

def error_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Event: OnError in {func.__name__}. Error: {str(e)}")
            # Implement custom error logic here (e.g., Send Mail)
            raise
    return wrapper

@error_handler
def data_flow_task():
    # task logic
    pass
```

### Pattern: OnPre/PostExecute (Context Manager)
```python
from contextlib import contextmanager
import time

@contextmanager
def task_lifecycle(task_name):
    # OnPreExecute
    print(f"Event: OnPreExecute for {task_name}")
    start_time = time.time()
    
    yield
    
    # OnPostExecute
    duration = time.time() - start_time
    print(f"Event: OnPostExecute for {task_name}. Duration: {duration:.2f}s")

def my_task():
    with task_lifecycle("MyTask"):
        # task logic
        pass
```

## Key Considerations
- **Scope**: SSIS event handlers bubble up the hierarchy. In Python, a global exception handler or middleware-style pattern can achieve similar results.
- **Variables**: Event handlers in SSIS have access to system variables like `ErrorCode` and `ErrorDescription`. These should be passed to the Python error handling functions.
