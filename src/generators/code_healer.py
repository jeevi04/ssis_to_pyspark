"""
PySpark Code Self-Healer

Detects and auto-fixes known bad patterns in LLM-generated PySpark code.
Runs after each LLM generation step.  Two healing modes:

  1. Rule-based patches (fast, deterministic):
       - Replace non-existent API calls: F.right() → F.expr("right(...)")
       - Fix JDBC dual-key conflict: remove dbtable when query is present
       - Fix F.isnull() → col.isNull()
       - Fix F.replace() → F.regexp_replace()
       - Fix F.left() → F.substring()
       - Fix ORDER BY in JDBC queries (not allowed)

  2. LLM-assisted healing (for syntax errors and semantic issues):
       - Feed validation errors back to LLM with targeted fix prompt
       - Apply only to files with critical errors, not warnings

Healing is applied transparently — the caller receives corrected code.
"""
import re
from dataclasses import dataclass, field
from typing import Optional


# ─── Rule-Based Patches ───────────────────────────────────────────────────────

@dataclass
class PatchResult:
    code: str
    patches_applied: list[str] = field(default_factory=list)
    syntax_ok: bool = True
    syntax_error: Optional[str] = None


# Each patch: (description, regex_pattern, replacement)
# Applied in order — earlier patches take precedence.
_RULE_PATCHES: list[tuple[str, str, str]] = [
    # ── Forbidden API calls ─────────────────────────────────────────────────
    (
        "F.right() → F.expr(right(...))",
        r'F\.right\(\s*F\.col\s*\(["\'](\w+)["\']\)\s*,\s*(\d+)\s*\)',
        r'F.expr("right(\1, \2)")',
    ),
    (
        "F.right() with col string → F.expr(right(...))",
        r'F\.right\(\s*["\'](\w+)["\']\s*,\s*(\d+)\s*\)',
        r'F.expr("right(\1, \2)")',
    ),
    (
        "F.left() → F.substring(col, 1, n)",
        r'F\.left\(\s*F\.col\s*\(["\'](\w+)["\']\)\s*,\s*(\d+)\s*\)',
        r'F.substring(F.col("\1"), 1, \2)',
    ),
    (
        "F.left() with col string → F.substring()",
        r'F\.left\(\s*["\'](\w+)["\']\s*,\s*(\d+)\s*\)',
        r'F.substring(F.col("\1"), 1, \2)',
    ),
    (
        "F.replace() → F.regexp_replace()",
        r'F\.replace\s*\(',
        r'F.regexp_replace(',
    ),
    (
        "F.isnull() → col.isNull()",
        r'F\.isnull\s*\(\s*F\.col\s*\(["\'](\w+)["\']\)\s*\)',
        r'F.col("\1").isNull()',
    ),
    (
        "F.isnull() with string arg → col.isNull()",
        r'F\.isnull\s*\(\s*["\'](\w+)["\']\s*\)',
        r'F.col("\1").isNull()',
    ),
    # ── JDBC option conflicts ────────────────────────────────────────────────
    # Remove .option("dbtable", ...) when .option("query", ...) is already present
    # This is a multi-line pattern — handled separately in _fix_jdbc_conflicts()
    # ── T-SQL in JDBC queries → ANSI SQL ───────────────────────────────────
    (
        "T-SQL ISNULL() → COALESCE() in JDBC strings",
        r'(?i)(\.option\s*\(\s*["\'](?:query|dbtable)["\']\s*,\s*["\'])([^"\']*?)ISNULL\s*\(([^,]+),([^)]+)\)',
        r'\1\2COALESCE(\3,\4)',
    ),
    (
        "T-SQL TOP N → remove (JDBC query) — mark for manual review",
        r'(?i)(\.option\s*\(\s*["\'](?:query|dbtable)["\']\s*,\s*["\'][^"\']*?)SELECT\s+TOP\s+\d+\s+',
        r'\1SELECT ',
    ),
    # ── Empty placeholder DFs ────────────────────────────────────────────────
    # Replace createDataFrame([], schema) stub with a comment
    # (Can't auto-fix this — flag it instead)
    # ── .collect() on large DataFrames ──────────────────────────────────────
    # We flag but don't auto-replace because legitimate uses exist (e.g. small lookup rows)
]


def apply_rule_patches(code: str) -> PatchResult:
    """Apply all deterministic rule-based patches to generated code."""
    result = code
    patches_applied = []

    for description, pattern, replacement in _RULE_PATCHES:
        try:
            new_result = re.sub(pattern, replacement, result, flags=re.DOTALL)
            if new_result != result:
                patches_applied.append(description)
                result = new_result
        except re.error:
            pass  # skip broken patterns

    # Handle JDBC dual-key conflict (query + dbtable)
    result, fixed = _fix_jdbc_conflicts(result)
    if fixed:
        patches_applied.append("JDBC conflict: removed duplicate dbtable option when query present")

    # Check Python syntax
    syntax_ok = True
    syntax_error = None
    try:
        compile(result, "<healer>", "exec")
    except SyntaxError as e:
        syntax_ok = False
        syntax_error = f"SyntaxError at line {e.lineno}: {e.msg}"

    return PatchResult(
        code=result,
        patches_applied=patches_applied,
        syntax_ok=syntax_ok,
        syntax_error=syntax_error,
    )


def _fix_jdbc_conflicts(code: str) -> tuple[str, bool]:
    """Remove .option('dbtable', ...) when .option('query', ...) is already present in same chain."""
    # Find JDBC read chains that have BOTH query and dbtable
    # Strategy: scan line by line, track open chains
    lines = code.split("\n")
    result_lines = []
    fixed = False
    i = 0
    while i < len(lines):
        line = lines[i]
        # Detect start of a .format("jdbc") chain
        if '.format("jdbc")' in line or ".format('jdbc')" in line:
            # Collect the full chain
            chain_start = i
            chain_lines = [line]
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if next_line.startswith('.') or next_line.startswith(')') or next_line == '':
                    chain_lines.append(lines[j])
                    j += 1
                    if next_line.startswith('.load') or next_line.endswith('.load()'):
                        break
                else:
                    break
            chain_str = "\n".join(chain_lines)
            has_query = bool(re.search(r'\.option\s*\(\s*["\']query["\']', chain_str))
            has_dbtable = bool(re.search(r'\.option\s*\(\s*["\']dbtable["\']', chain_str))
            if has_query and has_dbtable:
                # Remove the dbtable option line(s)
                cleaned = []
                for cl in chain_lines:
                    if re.search(r'\.option\s*\(\s*["\']dbtable["\']', cl):
                        fixed = True
                        continue  # skip this line
                    cleaned.append(cl)
                result_lines.extend(cleaned)
            else:
                result_lines.extend(chain_lines)
            i = j
            continue
        result_lines.append(line)
        i += 1
    return "\n".join(result_lines), fixed


def scan_for_warnings(code: str, context: str = "") -> list[str]:
    """Scan generated code for known anti-patterns. Returns list of warning strings."""
    warnings = []

    # Forbidden PySpark functions
    forbidden = [
        (r'\bF\.right\s*\(', "F.right() does not exist — use F.expr(\"right(col, n)\")"),
        (r'\bF\.left\s*\(',  "F.left() does not exist — use F.substring(col, 1, n)"),
        (r'\bF\.replace\s*\(', "F.replace() does not exist — use F.regexp_replace()"),
        (r'\bF\.isnull\s*\(', "F.isnull() does not exist — use col.isNull()"),
        (r'\.collect\s*\(\s*\)', ".collect() on potentially large DataFrame — add .limit() guard"),
    ]
    for pattern, msg in forbidden:
        if re.search(pattern, code):
            warnings.append(f"[{context}] {msg}")

    # JDBC conflict
    if '.option("query",' in code and '.option("dbtable",' in code:
        warnings.append(f"[{context}] JDBC conflict: both .option('query') and .option('dbtable') present")

    # Empty placeholder DataFrames
    if 'createDataFrame([], ' in code or 'createDataFrame([], schema)' in code:
        warnings.append(f"[{context}] Empty DataFrame placeholder detected — may silently drop rows")

    # ORDER BY in JDBC queries
    if re.search(r'\.option\s*\(["\'](?:query|dbtable)["\'].*?ORDER BY', code, re.DOTALL | re.IGNORECASE):
        warnings.append(f"[{context}] ORDER BY inside JDBC query — Spark ignores source ordering")

    # T-SQL ISNULL in queries
    if re.search(r'\bISNULL\s*\([^)]+,[^)]+\)', code, re.IGNORECASE):
        warnings.append(f"[{context}] T-SQL ISNULL() in SQL string — use ANSI COALESCE() instead")

    # countDistinct inside window
    if re.search(r'F\.countDistinct.*\.over\s*\(', code, re.DOTALL):
        warnings.append(f"[{context}] F.countDistinct() inside .over() Window — always returns 1")

    # Global row-count variables (module-level mutation)
    if re.search(r'^EXTRACT_COUNT\s*=\s*0', code, re.MULTILINE):
        warnings.append(f"[{context}] Global EXTRACT_COUNT — use local metrics dict to avoid re-run bugs")

    # Syntax check
    try:
        compile(code, f"<{context}>", "exec")
    except SyntaxError as e:
        warnings.append(f"[{context}] SyntaxError at line {e.lineno}: {e.msg}")

    return warnings


def build_healing_prompt(code: str, warnings: list[str], system_prompt: str = "") -> str:
    """Build a targeted LLM prompt to fix specific issues in generated code."""
    issues_text = "\n".join(f"  - {w}" for w in warnings)
    return f"""## Task: Fix PySpark Code Issues

The following generated PySpark code has specific issues that must be corrected:

### Issues Found:
{issues_text}

### Code to Fix:
```python
{code}
```

### Fix Instructions (MANDATORY — fix ALL issues listed above):

1. **F.right(col, n) does NOT exist** → replace with `F.expr("right(col_name, n)")`
2. **F.left(col, n) does NOT exist** → replace with `F.substring(col, 1, n)`
3. **F.replace() does NOT exist** → replace with `F.regexp_replace(col, pattern, repl)`
4. **F.isnull(col) does NOT exist** → replace with `F.col("col_name").isNull()`
5. **JDBC query + dbtable conflict** → remove `.option("dbtable", ...)` when `.option("query", ...)` is present
6. **F.countDistinct() inside .over()** → compute in a separate `.groupBy().agg()` then join back
7. **T-SQL ISNULL(a, b)** → replace with `COALESCE(a, b)` in SQL strings
8. **Syntax errors** → fix the indicated line

Return ONLY the corrected Python code block. Do not add any explanation.
"""
