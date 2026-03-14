# Control Flow Conversion Rules

## Overview

SSIS control flow elements (Sequence Containers, ForEach/For Loops, Precedence Constraints,
Execute SQL Tasks, Script Tasks) map to Python control structures and PySpark operations.

---

## Precedence Constraints → Python Try/Except

| SSIS Constraint | Python Equivalent |
|---|---|
| Value = 0 (Success) | `try: ... ` next step after success |
| Value = 1 (Failure) | `except Exception as e:` |
| Value = 2 (Completion) | `finally:` |
| `EvalOp = Expression` | `if <condition>:` |
| `EvalOp = ExpressionAndConstraint` | `if success and <condition>:` |
| `EvalOp = ExpressionOrConstraint` | `if success or <condition>:` |
| `LogicalAnd = False` | Multiple parallel branches connected by `or` |

### Pattern

```python
def run_pipeline(spark):
    # TSK_Step1 → (Success) → TSK_Step2
    try:
        result_1 = run_step1(spark)
    except Exception as e:
        logger.error(f"Step1 failed: {e}", exc_info=True)
        raise  # hard-stop — downstream cannot run

    try:
        run_step2(spark, result_1)
    except Exception as e:
        logger.error(f"Step2 failed: {e}", exc_info=True)
        # continue if constraint is Completion, raise if Success-only
```

---

## Sequence Container → Python Function

A Sequence Container groups tasks that run sequentially. Map it to a Python function:

```python
def seq_main(spark: SparkSession) -> None:
    """SSIS: CON_SEQ_Main — Sequence Container"""
    tsk_step_a(spark)
    tsk_step_b(spark)
    tsk_step_c(spark)
```

---

## ForEach Loop Container → Python `for` Loop

| SSIS Enumerator | Python Equivalent |
|---|---|
| `ForEachADOEnumerator` | `for row in dataframe.collect():` |
| `ForEachFileEnumerator` | `for f in Path(dir).glob(pattern):` |
| `ForEachFromVariableEnumerator` | `for item in variable_list:` |
| `ForEachNodeListEnumerator` | `for node in xml_doc.findall(xpath):` |

### ForEach ADO (DataFrame rows) Pattern

```python
# SSIS: ForEachLoop over an ADO object variable (result of Execute SQL Task)
# Variable mapping: Row → @[User::CurrentItemID]

batch_df = spark.read.format("jdbc") \
    .option("query", "SELECT item_id FROM schema.Order WHERE status = 'PENDING'") \
    ...load()

for row in batch_df.collect():
    current_item_id = row["item_id"]
    logger.info(f"Processing item: {current_item_id}")
    process_item(spark, current_item_id, jdbc_config)
```

### ForEach File Pattern

```python
from pathlib import Path

# SSIS: ForEachFileEnumerator, directory=/data/input, mask=*.csv
for file_path in sorted(Path("/data/input").glob("*.csv")):
    logger.info(f"Processing file: {file_path.name}")
    df = spark.read.csv(str(file_path), header=True, inferSchema=True)
    process_file(spark, df, file_path.name)
```

---

## For Loop Container → Python `while` / `for range()`

```python
# SSIS: ForLoop — InitExpression=@i=0, EvalExpression=@i<10, AssignExpression=@i=@i+1
i = 0
while i < 10:
    process_iteration(spark, i)
    i += 1

# Or equivalently:
for i in range(0, 10):
    process_iteration(spark, i)
```

---

## Execute SQL Task → spark.sql() or JDBC execute

| SSIS ResultSet | Python Equivalent |
|---|---|
| None (DDL/DML) | `jdbc_conn.execute(sql)` |
| Single row → variable | `row = spark.read.jdbc(query=sql).collect()[0]`|
| Full result → object variable | `df = spark.read.format("jdbc").option("query", sql).load()` |
| XML result | Parse result and iterate nodes |

### DDL / DML Execute SQL Pattern

```python
def execute_sql_task(jdbc_config: dict, sql: str, task_name: str) -> None:
    """Execute a non-returning SQL statement via JDBC.
    SSIS equivalent: Execute SQL Task (ResultSet = None).
    """
    import jaydebeapi  # or use pyodbc, turbodbc, etc.
    logger.info(f"Execute SQL Task: {task_name}")
    # For Databricks: use spark.sql() for catalog operations
    # For SQL Server DDL: use JDBC connection
    # Example using spark for Hive catalog:
    # spark.sql(sql)
    logger.info(f"Completed SQL Task: {task_name}")
    # TODO: Replace with actual JDBC execute for SQL Server DDL statements
```

### Execute SQL → Single Row Variable Pattern

```python
def get_lookup_data(spark: SparkSession, jdbc_config: dict) -> dict:
    """SSIS: Execute SQL Task returning a single row to variables."""
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_config["url"])
        .option("query", "(SELECT TOP 1 id, status FROM schema.ControlTable ORDER BY id DESC) as c")
        .option("driver", jdbc_config["driver"])
        .option("user", jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .load()
    )
    if df.count() == 0:
        return {}
    row = df.collect()[0]
    return {"id": row["id"], "status": row["status"]}
```

---

## Script Task → Python Function with TODO

SSIS Script Tasks contain C# or VB.NET code that must be manually reviewed.

```python
def script_task_<task_name>(spark: SparkSession) -> None:
    """SSIS: Script Task — <TaskName>

    Original C# logic (auto-detected inputs/outputs):
      ReadOnlyVariables:  <var1>, <var2>
      ReadWriteVariables: <var3>

    # TODO: Review and translate the following C# logic:
    # <original_script_code>
    """
    logger.warning("Script Task '<task_name>' requires manual translation — auto-stub only")
    raise NotImplementedError(
        "Script Task '<task_name>' must be manually implemented. "
        "See docstring for original C# code."
    )
```

---

## Event Handlers → try/except

| SSIS Event Handler | Python Equivalent |
|---|---|
| `OnError` | `except Exception:` |
| `OnTaskFailed` | `except TaskFailedException:` |
| `OnPostExecute` | `finally:` |
| `OnWarning` | `logger.warning(...)` |

```python
try:
    run_data_flow(spark)
except Exception as e:
    # SSIS: OnError event handler
    logger.error(f"OnError: {e}", exc_info=True)
    send_failure_notification(str(e))  # SSIS: Send Mail Task in OnError handler
    raise
finally:
    # SSIS: OnPostExecute event handler
    log_package_end(spark)
```

---

## Important Rules

1. **Precedence = Success → try/except hard-stop**: Downstream steps MUST NOT run if upstream fails.
2. **ForEach ADO → prefer Spark over row-by-row**: If the ForEach loop processes a DataFrame row by row,  
   refactor it into a vectorized Spark operation to avoid the performance penalty of `.collect()`.
3. **Script Tasks = always TODO**: Never auto-generate logic for Script Tasks — flag them for manual review.
4. **Execute SQL = prefer spark.sql()** for catalog/DDL operations in Databricks; use JDBC for SQL Server-specific DDL.
5. **Nested containers** → one Python function per container level, called by the parent function.
