# SSIS Environment References - Knowledge Base

## Purpose
Environment references allow a project to use values from environments (collections of variables) stored in the SSISDB catalog. This facilitates moving packages between DEV, TEST, and PROD.

## PySpark Equivalent
In most modern data platforms (Databricks, Fabric, etc.), this is handled using environment-specific configuration files or environment variables.

### Pattern: .env Files
```python
from dotenv import load_dotenv
import os

def initialize_environment(stage="dev"):
    # Load environment variables from a .env file
    env_file = f"env/.env.{stage}"
    load_dotenv(env_file)
    
    # Access variables
    db_host = os.getenv("DATABASE_HOST")
    secret_key = os.getenv("API_KEY")
```

### Pattern: Secret Scopes (Databricks)
```python
def get_env_variable(scope, key):
    return dbutils.secrets.get(scope=scope, key=key)
```

## Key Considerations
- **SSISDB Mapping**: In SSIS, environment variables map to package/project parameters. In Python, this is more direct via `os.getenv` or a config dictionary.
- **CI/CD Integration**: Ensure the environment-specific values are injected by the build/release pipeline (e.g., Azure DevOps, GitHub Actions).
- **Default Values**: Provide sensible defaults in your Python code for local development.
