"""
Pytest configuration and shared fixtures.
"""
import os
from pathlib import Path

import pytest

# Set test environment
os.environ["APP_ENVIRONMENT"] = "test"


@pytest.fixture
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def fixtures_dir():
    """Return the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_xml(fixtures_dir):
    """Return path to sample XML file."""
    return fixtures_dir / "sample_filter.xml"


@pytest.fixture
def mock_llm_response():
    """Return a mock LLM response."""
    return """
```python
def filter_male_employees(input_df: DataFrame) -> DataFrame:
    \"\"\"Filter to select only male employees.\"\"\"
    return input_df.filter(F.col("GENDER") == "M")
```
"""
