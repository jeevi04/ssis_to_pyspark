# Sample SSIS Package Files

Place your SSIS package files (.dtsx) in this directory.

## How to Export from SQL Server Integration Services

1. Open **SQL Server Data Tools (SSDT)** or **Visual Studio**
2. Navigate to your SSIS project
3. Locate the package (.dtsx file) you want to convert
4. Copy the .dtsx file to this directory

## File Format

SSIS packages are XML-based files with `.dtsx` extension containing:
- Data Flow Tasks
- Control Flow Tasks
- Connection Managers
- Variables and Parameters
- Event Handlers

## Testing

```bash
uv run ssis2spark convert Input/your_package.dtsx -o output/
```
