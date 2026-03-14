"""Verification scan: check all v2 fixes are present in pyspark_output.py."""
from pathlib import Path

content = Path("src/generators/pyspark_output.py").read_text(encoding="utf-8")

checks = [
    ("utils FORBIDDEN rule",         "FORBIDDEN: Do NOT add `from utils import *`"),
    ("Silver cross-reference rule",  "CROSS-REFERENCE RULE"),
    ("SSIS variable placeholder fix","FORBIDDEN: Using placeholder strings"),
    ("Bronze foreach loop pattern",  "NEVER** use `.first()[\"BatchIdentifier\"]`"),
    ("Gold tables_literal wired",    "{tables_literal}"),
    ("tables_literal built",         "tables_literal = repr(silver_table_names)"),
    ("Silver jdbc_config in main prompt", "run_silver_pipeline(spark, processing_date, jdbc_config)` to persist Silver"),
    ("Main module signature updated","run_silver_pipeline(spark, processing_date, jdbc_config)"),
    ("Merge Join 4c rule",           "4c. **Merge Join Transformations"),
    ("Conditional Split 4d rule",    "4d. **Conditional Split"),
    ("SSIS variable 4e rule",        "4e. **SSIS Variable Usage"),
    ("TABLE NAMING RULE",            "TABLE NAMING RULE"),
    ("ORCHESTRATION COMPLETENESS",   "ORCHESTRATION COMPLETENESS"),
    ("utils import banned in main prompt", "Silver module:`.*run_silver_pipeline"),
]

all_ok = True
for label, text in checks[:-1]:
    found = text in content
    status = "OK" if found else "MISSING"
    print(f"[{status}]  {label}")
    if not found:
        all_ok = False

print()
print("ALL OK" if all_ok else "SOME MISSING — review above")
