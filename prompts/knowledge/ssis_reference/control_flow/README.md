# SSIS Control Flow - Knowledge Base

This section documents how to convert SSIS Control Flow elements (Tasks, Containers, and Precedence Constraints) into Python control flow.

## Sections

- **[Rules & Patterns](rules.md)**: Detailed conversion rules for precedence constraints and core task mapping.
- **[Event Handlers](event_handlers.md)**: Mapping OnError, OnPreExecute, etc.
- **[CDC Control Task](tasks/cdc_control.md)**: Managing Change Data Capture state.
- **[XML Task](tasks/xml_task.md)**: Handling XML operations.
- **[Expression Task](tasks/expression_task.md)**: Simple variable assignments and logic.
- **[Web Service Task](tasks/web_service.md)**: Calling external APIs.
- **[Execute Package Task](tasks/execute_package.md)**: Orchestrating child packages.
- **[Execute Process Task](tasks/execute_process.md)**: Running external scripts.
- **[File System Task](tasks/file_system.md)**: File and folder operations.
- **[Send Mail Task](tasks/send_mail.md)**: Notifications.
- **[Script Task](tasks/script_task.md)**: Custom Control Flow logic.

## Conversion Process
1. **Graph Reconstruction**: The rule engine builds a directed graph of tasks based on `PrecedenceConstraints`.
2. **Function Definition**: Each task and container is defined as a Python function.
3. **Execution Logic**: Tasks are called in sequence, wrapped in `try-except-finally` blocks to honor success/failure/completion constraints.
