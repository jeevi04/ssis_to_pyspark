# SSIS Project Parameters - Knowledge Base

## Purpose
Project parameters allow you to provide values to properties within one or more packages in a project. They are defined at the project level and can be shared across all packages.

## PySpark Equivalent
In a PySpark/Python project, these are typically handled using a centralized configuration file (e.g., `config.json`, `settings.yml`) or environment variables.

### Pattern: Centralized Config
```python
import json
import os

def load_project_parameters(env="dev"):
    config_path = f"config/project_parameters_{env}.json"
    with open(config_path, 'r') as f:
        return json.load(f)

# Usage in packages
project_params = load_project_parameters()
db_connection = project_params['ConnectionString']
```

## Key Considerations
- **Sensitive Data**: Parameters marked as `Sensitive` in SSIS should be stored in a secure secrets manager (e.g., Azure Key Vault, AWS Secrets Manager, Databricks Secrets) and accessed via API.
- **Data Types**: Ensure the Python configuration parser correctly handles types (integers, booleans) to match SSIS parameter types.
- **Overrides**: SSIS allows overriding project parameters at runtime via SQL Agent or Command Line. In Python, use command-line arguments (e.g., `argparse`) to override default config values.
