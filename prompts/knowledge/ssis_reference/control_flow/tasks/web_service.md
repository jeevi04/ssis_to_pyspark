# SSIS Web Service Task - Knowledge Base

This document provides conversion patterns for the SSIS Web Service Task.

## Purpose
The Web Service Task executes a web service method and stores the result in a variable or file. It typically uses WSDL (Web Services Description Language) to define the service interface.

## PySpark Equivalent
In Python, web services are typically called using the `requests` library (for REST) or `zeep` library (for SOAP/WSDL).

### Pattern 1: REST API Call
```python
import requests

def web_service_task_rest(url, headers, data):
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response.json()
```

### Pattern 2: SOAP / WSDL Call
Requires the `zeep` library.
```python
from zeep import Client

def web_service_task_soap(wsdl_url, method_name, parameters):
    client = Client(wsdl=wsdl_url)
    method = getattr(client.service, method_name)
    result = method(**parameters)
    return result
```

## Key Considerations
- **Authentication**: Python scripts need to handle OAuth, Basic Auth, or API Keys explicitly via headers or dedicated libraries.
- **WSDL Mapping**: SSIS maps WSDL methods to task properties. In Python, you must manually define the method name and parameter mappings.
- **Result Storage**: Ensure the result (JSON/XML/String) is correctly parsed and stored in the desired Python variable.
