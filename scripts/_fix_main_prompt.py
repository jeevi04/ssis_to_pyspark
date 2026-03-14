"""Patch the main orchestration prompt to include jdbc_config in Silver call."""
from pathlib import Path

target = Path("src/generators/pyspark_output.py")
content = target.read_text(encoding="utf-8")

# Exact strings captured from repr() output
old_c = "     c. Accept `processing_date` parameter (maps to SSIS variable `User::ProcessingDate`), pass it to `run_silver_pipeline(spark, processing_date)`\n"
old_d = "     d. Phase 1 Silver: Call `run_silver_pipeline(spark, processing_date)` to persist Silver tables \u2014 ONLY if Bronze succeeds\n"
old = old_c + old_d

new = (
    "     c. Accept `processing_date` parameter (maps to SSIS variable `User::ProcessingDate`)\n"
    "     d. Phase 1 Silver: Call `run_silver_pipeline(spark, processing_date, jdbc_config)` to persist Silver tables \u2014 ONLY if Bronze succeeds\n"
    "        - **CRITICAL**: Always pass `jdbc_config` as the third argument. Silver lookup functions require JDBC access. Omitting it causes `NoneType` errors in lookup transforms.\n"
)

count = content.count(old)
print(f"Occurrences: {count}")

if count == 1:
    content = content.replace(old, new)
    target.write_text(content, encoding="utf-8")
    print("REPLACED OK")
elif count > 1:
    print("ERROR: multiple matches — unsafe to replace")
else:
    print("NOT FOUND")
    # Also patch the in-code template example that uses same signature
    old2 = "             run_silver_pipeline(spark, processing_date)\n"
    count2 = content.count(old2)
    print(f"Template example occurrences: {count2}")
    if count2 >= 1:
        new2 = "             run_silver_pipeline(spark, processing_date, jdbc_config)\n"
        content = content.replace(old2, new2, 1)
        target.write_text(content, encoding="utf-8")
        print("Template example REPLACED OK")
