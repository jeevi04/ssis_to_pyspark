"""
PySpark Unit Test Generator.

Generates pytest unit tests for PySpark code based on the business logic
extracted from SSIS packages.
"""
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from src.config import get_config
from src.llm import BaseLLMProvider
from src.logging import get_logger, LogContext
from src.parsers import Mapping, Workflow

logger = get_logger(__name__)


@dataclass
class TestGenerationResult:
    """Result of unit test generation."""
    success: bool
    files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class PySparkUnitTestGenerator:
    """
    Generates pytest unit tests for PySpark modules created from SSIS package mappings.
    
    Strategy:
    1. Analyze the mapping logic
    2. Identify critical business rules (filters, joins, calculations, aggregations)
    3. Generate pytest tests that validate these rules with sample data
    4. Create fixtures and helper functions for test setup
    """
    
    def __init__(self, llm_provider: BaseLLMProvider, verbose: bool = False):
        self.llm = llm_provider
        self.verbose = verbose
        self.config = get_config()
        
        # Load system prompt
        self.system_prompt = self.llm.load_prompt("system.md")
    
    def generate(self, workflow: Workflow, output_dir: Path) -> TestGenerationResult:
        """
        Generate unit tests for all mappings in a workflow.
        
        Args:
            workflow: Parsed SSIS package
            output_dir: Directory to write tests (should be the 'tests' directory)
            
        Returns:
            TestGenerationResult with file paths and any errors
        """
        result = TestGenerationResult(success=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with LogContext(workflow=workflow.name):
            logger.info("Starting unit test generation", workflow=workflow.name)
            
            # Parent module name for imports (matches PySparkGenerator convention)
            pkg_name = self._sanitize_filename(workflow.name)
            parent_module = f"silver_{pkg_name}"
            
            # Generate one test file for each mapping
            for mapping in workflow.mappings:
                try:
                    file_path = self._generate_mapping_test(mapping, output_dir, parent_module)
                    if file_path:
                        result.files.append(str(file_path))
                        logger.info("Generated test for mapping", mapping=mapping.name, file=str(file_path))
                except Exception as e:
                    error_msg = f"Error generating test for {mapping.name}: {e}"
                    result.errors.append(error_msg)
                    result.success = False
                    logger.error("Test generation failed", mapping=mapping.name, error=str(e))
        
        return result
    
    def _generate_mapping_test(self, mapping: Mapping, output_dir: Path, parent_module: str) -> Path:
        """Generate pytest test file for a mapping."""
        # Sanitize mapping name for the test filename
        test_file_base = self._sanitize_filename(mapping.name)
        
        # Build prompt for test generation
        prompt = self._build_test_prompt(mapping, test_file_base, parent_module)
        
        # Generate via LLM
        response = self.llm.generate(prompt, self.system_prompt)
        
        # Extract Python code
        test_code = self.llm.extract_code(response.text)
        
        # Write file
        filename = f"test_{test_file_base}.py"
        file_path = output_dir / filename
        file_path.write_text(test_code, encoding='utf-8')
        
        return file_path
    
    def _build_test_prompt(self, mapping: Mapping, test_file_base: str, parent_module: str) -> str:
        """Build prompt for LLM to generate pytest tests."""
        # Extract business logic context
        mapping_name_sanitized = self._sanitize_filename(mapping.name)
        
        # Transformation functions in Silver are usually named transform_<sanitized_mapping_name>
        expected_function = f"transform_{mapping_name_sanitized}"
        transformations_info = []
        for tx in mapping.transformations:
            tx_info = {
                "name": tx.name,
                "type": tx.type,
                "filter": tx.filter_condition,
                "join": tx.join_condition,
                "lookup": tx.lookup_condition,
                "group_by": tx.group_by,
                "fields": [
                    {
                        "name": f.name,
                        "type": f.datatype,
                        "expression": f.expression
                    }
                    for f in tx.fields[:10] if f.port_type == "OUTPUT"
                ]
            }
            transformations_info.append(tx_info)

        prompt = f"""
## Task: Generate Complete Self-Contained PySpark Unit Test File

You are a QA Engineer specialized in data pipeline testing. Your task is to generate a comprehensive, **self-contained** pytest test file for a PySpark module that was converted from an SSIS package.

### Module Information:
    - **PySpark Module Under Test:** {parent_module}.py
    - **Original SSIS Mapping:** {mapping.name}
    - **Expected Transformation Function:** {expected_function} (or similarly named based on the mapping)

```json
{json.dumps(transformations_info, indent=2)}
```

### CRITICAL - Function Naming:
- Use EXACTLY `{expected_function}` for the transformation function name.
- Do NOT add extra words like "to" or "stage" to the function name (e.g., if `{expected_function}` is `transform_foo`, do NOT use `transform_foo_to_stage`).

## Requirements for the Test File:

1. **Self-Contained Structure:**
   - Create ONE complete pytest file: `test_{test_file_base}.py`
   - Include ALL fixtures, helpers, and utilities within this single file
   - No external dependencies except pytest, PySpark, and the module under test
   - The file should be runnable with just `pytest test_{test_file_base}.py`

2. **File Organization:**
   ```python
   # Module docstring
   # Imports
   # Helper functions and utilities
   # pytest fixtures
   # Test classes/functions
   ```

3. **Imports:**
    - Import pytest
    - Import PySpark modules (SparkSession, DataFrame, functions as F, types, Window)
    - **CRITICAL**: Import the parent module: `from {parent_module} import *`
    - **NO PLACEHOLDER IMPORTS**: Do NOT write `from dft_mapping import *` — the code is in `{parent_module}.py`.
    - If you need a specific transform function, it should be in `{parent_module}.py`.

4. **Helper Functions (include in the same file):**
   - `assert_dataframe_equal()`: Compare two DataFrames for equality
   - `create_test_spark()`: Create a SparkSession configured for testing
   - Any other helper functions needed for DataFrame comparisons or test data generation

5a. **CRITICAL — pytest Fixture Rules (read carefully):**
   - pytest fixtures are **dependency-injected by name** via function parameters — they are NEVER called directly as functions.
   - **WRONG**: `expected_df = expected_output_data(spark)` inside a test method
   - **CORRECT**: Add `expected_output_data` as a parameter: `def test_foo(self, spark, sample_input_data, expected_output_data):`
   - Every fixture used in a test class method MUST appear as a parameter of that method.
   - Fixtures used in standalone test functions MUST appear as parameters of that function.

5. **pytest Fixtures (include in the same file):**
   - `spark`: Session-scoped SparkSession fixture
   - Sample input DataFrame fixtures with realistic test data
   - Expected output DataFrame fixtures
   - Design fixtures that cover edge cases (null values, empty sets, boundary conditions)

6. **Test Cases:**
   Based on the transformations above, generate tests for:
   
   **Data Quality Tests:**
   - Test for null handling and default values
   - Test data type conversions
   - Test schema validation
   
   **Business Logic Tests:**
   - Test filter conditions (if any filters exist)
   - Test join logic (if any joins exist)
   - Test lookup operations (if any lookups exist)
   - Test aggregations (if any aggregations exist)
   - Test calculated fields and expressions
   
   **Edge Case Tests:**
   - Test with empty DataFrames
   - Test with null values in critical fields
   - Test with duplicate records
   - Test boundary conditions for numeric calculations
   - **CRITICAL — Null assertions must reflect actual business logic**: If the transformation maps `NULL` to a default value (e.g. `NULL gender → "U"`, `NULL flag → False`), then after the transform:
     - The output column will NOT be null — assert `count == 0` for null filter
     - Assert the actual default value is present instead
     - Do NOT assert `isNull().count() == 1` when the logic explicitly fills nulls with a default
   - Also: when creating test DataFrames for a test that only populates one column, include ALL columns required by the transformation function (match the expected schema), otherwise the function will fail with `AnalysisException`.
   
   **Integration Tests:**
   - Test the complete `run_mapping()` function end-to-end
   - Verify expected row counts
   - Verify expected column values

7. **Test Data:**
   - Create realistic sample data that represents typical input
   - Include edge cases in the test data
   - Use meaningful values that make tests readable
   - Create both positive (should pass) and negative (should fail if logic is wrong) test scenarios

8. **Assertions:**
   - Use appropriate pytest assertions
   - Check row counts: `assert df.count() == expected_count`
   - Check column values: `assert df.select("col").collect()[0][0] == expected_value`
   - Check for presence/absence of null values
   - Use the custom `assert_dataframe_equal()` helper for complete DataFrame comparisons

9. **Test Organization:**
   - Group related tests into classes if appropriate
   - Use descriptive test names: `test_<functionality>_<scenario>`
   - Add parametrized tests for similar scenarios with different inputs
   - Include docstrings explaining what each test validates

10. **Code Quality:**
   - Follow PEP 8 style guidelines
   - Use type hints for functions
   - Add comments for complex test setup
   - Make tests independent and idempotent
   - Ensure tests are deterministic (no random data without seeds)

11. **Example Structure:**
```python
\"\"\"
Unit tests for {parent_module} PySpark module.

Tests the business logic converted from SSIS package mapping: {mapping.name}
\"\"\"
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import List

# Import EXACTLY the expected function
from {parent_module} import {expected_function}


# ============================================================================
# Helper Functions
# ============================================================================

def assert_dataframe_equal(df1: DataFrame, df2: DataFrame, check_order: bool = False):
    \"\"\"Assert two DataFrames are equal.\"\"\"
    # Implementation here
    pass


def create_test_spark() -> SparkSession:
    \"\"\"Create SparkSession for testing.\"\"\"
    return SparkSession.builder \\
        .master("local[1]") \\
        .appName("test") \\
        .config("spark.ui.enabled", "false") \\
        .config("spark.sql.shuffle.partitions", "1") \\
        .getOrCreate()


# ============================================================================
# pytest Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    \"\"\"Provide SparkSession for all tests.\"\"\"
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_input_data(spark):
    \"\"\"Create sample input DataFrame for testing.\"\"\"
    data = [
        # Your test data here
    ]
    schema = StructType([
        # Your schema here
    ])
    return spark.createDataFrame(data, schema)


# ============================================================================
# Test Cases
# ============================================================================

class TestBusinessLogic:
    \"\"\"Test business logic and transformations.\"\"\"
    
    def test_filter_logic(self, spark):
        \"\"\"Test that filter conditions are applied correctly.\"\"\"
        # Arrange
        input_data = [...]
        input_df = spark.createDataFrame(input_data, schema)
        
        # Act
        result_df = some_function(input_df)
        
        # Assert
        assert result_df.count() == expected_count
    
    # More tests...


def test_end_to_end_pipeline(spark, sample_input_data, expected_output_data):  # inject fixtures as params
    \"\"\"Test the complete mapping pipeline.\"\"\"
    # Arrange — expected_output_data is a fixture injected above, not called directly

    # Act — run_mapping takes an input DataFrame, NOT the spark session
    result_df = run_mapping(sample_input_data)

    # Assert
    assert result_df.count() == expected_output_data.count()
```

## Output:
Generate ONE complete, self-contained pytest file with:
- Comprehensive module docstring
- All necessary imports
- Helper functions for DataFrame testing
- All pytest fixtures
- Multiple test functions covering different aspects of the business logic
- Realistic test data
- Clear assertions and error messages
- Comments explaining complex test scenarios

The test file should be production-ready, comprehensive, runnable standalone, and follow pytest best practices.
"""
        return prompt
    
    def _sanitize_filename(self, name: str) -> str:
        """Convert name to valid Python module name (matches PySparkGenerator logic)."""
        sanitized = "".join(c if c.isalnum() else "_" for c in name)
        while "__" in sanitized:
            sanitized = sanitized.replace("__", "_")
        sanitized = sanitized.strip("_").lower()
        if sanitized and sanitized[0].isdigit():
            sanitized = "m_" + sanitized
        return sanitized