"""
SSIS to PySpark Migration Accelerator CLI.

Command-line interface for converting SSIS packages to PySpark.
"""
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Load environment variables early
load_dotenv()

from src.config import get_config
from src.generators import (
    BusinessLogicDocGenerator,
    PySparkGenerator,
    PySparkUnitTestGenerator,
    ConversionLogGenerator,
    DataModelReportGenerator
)
from src.llm import create_provider, get_available_providers
from src.logging import setup_logging, get_logger
from src.parsers import SSISParser

app = typer.Typer(
    name="ssis2spark",
    help="LLM-powered SSIS to PySpark migration accelerator",
    add_completion=False,
)
console = Console()


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
):
    """Initialize logging."""
    level = "DEBUG" if verbose else "INFO"
    setup_logging(level)


# ─── Core conversion logic (shared by convert & convert-batch) ───────

def _convert_single_file(
    input_file: Path,
    output_dir: Path,
    provider: Optional[str],
    verbose: bool,
    llm=None,
) -> bool:
    """
    Convert a single SSIS .dtsx file to PySpark.

    If an LLM provider is passed in, reuses it (batch mode).
    Otherwise creates a new one (single-file mode).

    Returns True on success, False on failure.
    """
    logger = get_logger("convert")

    project_name = input_file.stem
    project_dir = output_dir / project_name
    project_dir.mkdir(parents=True, exist_ok=True)

    test_dir = project_dir / "test"
    doc_dir = project_dir / "doc"
    test_dir.mkdir(parents=True, exist_ok=True)
    doc_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Step 1: Parse SSIS Package
        console.print(f"\n[yellow]Step 1:[/yellow] Parsing SSIS package...")
        parser = SSISParser()
        workflow = parser.parse(input_file)

        if workflow.name in ("Imported Workflow", "Parsed Workflow", "Unknown", ""):
            workflow.name = project_name

        console.print(f"  [green]OK[/green] Found {len(workflow.mappings)} mapping(s)")
        console.print(f"  [green]OK[/green] Found {len(workflow.transformations)} transformation(s)")

        # Step 2: Initialize LLM provider (reuse if provided)
        if llm is None:
            console.print(f"\n[yellow]Step 2:[/yellow] Initializing LLM provider...")
            llm = create_provider(provider=provider)

            if not llm.health_check():
                console.print(f"  [red]✗ Provider {llm.provider_name} is not available[/red]")
                if llm.provider_name == "ollama":
                    console.print("\n[yellow]Hint:[/yellow] Make sure Ollama is running:")
                    console.print("  1. Install Ollama: https://ollama.ai/download")
                    console.print("  2. Start Ollama: ollama serve")
                    console.print("  3. Pull model: ollama pull llama3.2")
                return False
        else:
            console.print(f"\n[yellow]Step 2:[/yellow] Reusing LLM provider ({llm.provider_name})")

        console.print(f"  [green]OK[/green] Using {llm.provider_name} ({llm.model})")

        # Step 3: Generate PySpark code (Medallion Architecture: Silver + Gold)
        console.print(f"\n[yellow]Step 3:[/yellow] Generating PySpark code (Medallion Architecture)...")
        generator = PySparkGenerator(llm_provider=llm, verbose=verbose)
        result = generator.generate(workflow, project_dir)

        if result.errors:
            for error in result.errors:
                console.print(f"  [red]Error: {error}[/red]")

        console.print(f"  [green]OK[/green] Generated {len(result.files)} file(s) (Silver + Gold + Main + Utils)")

        # Step 4: Generate Unit Tests
        console.print(f"\n[yellow]Step 4:[/yellow] Generating unit tests...")
        test_generator = PySparkUnitTestGenerator(llm_provider=llm, verbose=verbose)
        test_result = test_generator.generate(workflow, test_dir)

        if test_result.errors:
            for error in test_result.errors:
                console.print(f"  [red]Error: {error}[/red]")

        console.print(f"  [green]OK[/green] Generated {len(test_result.files)} unit test file(s)")

        # Step 5: Generate Documentation
        console.print(f"\n[yellow]Step 5:[/yellow] Generating business logic documentation...")
        doc_generator = BusinessLogicDocGenerator(llm_provider=llm, verbose=verbose)
        doc_result = doc_generator.generate(workflow, doc_dir)

        if doc_result.errors:
            for error in doc_result.errors:
                console.print(f"  [red]Error: {error}[/red]")

        console.print(f"  [green]OK[/green] Generated {len(doc_result.files)} documentation file(s)")

        # Step 6: Generate Conversion Log
        console.print(f"\n[yellow]Step 6:[/yellow] Generating conversion log (KT document)...")
        log_generator = ConversionLogGenerator(llm_provider=llm, verbose=verbose)
        log_result = log_generator.generate(workflow, result.metadata, doc_dir)

        if log_result.errors:
            for error in log_result.errors:
                console.print(f"  [red]Error: {error}[/red]")

        console.print(f"  [green]OK[/green] Generated {len(log_result.files)} conversion log file(s)")

        # Step 7: Generate Data Model Report
        console.print(f"\n[yellow]Step 7:[/yellow] Generating data model report (source/target tables & columns)...")
        dm_generator = DataModelReportGenerator(verbose=verbose)
        dm_result = dm_generator.generate(workflow, doc_dir)

        if dm_result.errors:
            for error in dm_result.errors:
                console.print(f"  [red]Error: {error}[/red]")

        console.print(f"  [green]OK[/green] Generated {len(dm_result.files)} data model report file(s)")

        # Summary
        all_success = (
            result.success
            and test_result.success
            and doc_result.success
            and log_result.success
            and dm_result.success
        )
        if all_success:
            console.print(f"\n[green]OK: Conversion complete for {project_name}![/green]")
        else:
            console.print(f"\n[yellow]Warning: {project_name} completed with errors[/yellow]")

        console.print(f"\n[bold]Output directory:[/bold] {project_dir.absolute()}")
        all_files = result.files + test_result.files + doc_result.files + log_result.files + dm_result.files
        for f in all_files:
            try:
                console.print(f"  • {Path(f).relative_to(project_dir)}")
            except ValueError:
                console.print(f"  • {f}")

        return all_success

    except Exception as e:
        logger.exception(f"Conversion failed for {input_file.name}")
        console.print(f"\n[red]Error converting {input_file.name}: {e}[/red]")
        return False


# ─── CLI Commands ─────────────────────────────────────────────────────

@app.command()
def convert(
    input_path: Path = typer.Argument(
        ...,
        help="Path to a .dtsx file OR a folder containing .dtsx files",
    ),
    output_dir: Path = typer.Option("./output", "--output", "-o", help="Output directory"),
    provider: Optional[str] = typer.Option(None, "--provider", "-p", help="LLM provider (ollama, gemini, anthropic)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed progress"),
):
    """
    Convert SSIS package(s) to PySpark code (Medallion Architecture: Silver + Gold).

    Accepts either:
      - A single .dtsx file
      - A folder (processes all .dtsx files in it and subfolders recursively)
    """
    console.print("\n[bold blue]>> SSIS to PySpark Converter[/bold blue]\n")

    if not input_path.exists():
        console.print(f"[red]Error: Input path not found: {input_path}[/red]")
        raise typer.Exit(1)

    # Discover files to process
    if input_path.is_file():
        if input_path.suffix.lower() != ".dtsx":
            console.print(f"[red]Error: Expected a .dtsx file, got: {input_path.suffix}[/red]")
            raise typer.Exit(1)
        dtsx_files = [input_path]
    else:
        # It's a directory — find all .dtsx files recursively
        dtsx_files = sorted(input_path.rglob("*.dtsx"))
        if not dtsx_files:
            console.print(f"[yellow]No .dtsx files found in {input_path}[/yellow]")
            raise typer.Exit(0)

    console.print(f"  Found [cyan]{len(dtsx_files)}[/cyan] SSIS package(s) to convert\n")

    # Initialize LLM provider once for batch efficiency
    llm = None
    if len(dtsx_files) > 1:
        console.print("[yellow]Initializing LLM provider for batch processing...[/yellow]")
        try:
            llm = create_provider(provider=provider)
            if not llm.health_check():
                console.print(f"  [red]✗ Provider {llm.provider_name} is not available[/red]")
                raise typer.Exit(1)
            console.print(f"  [green]OK[/green] Using {llm.provider_name} ({llm.model})\n")
        except Exception as e:
            console.print(f"[red]Error initializing LLM: {e}[/red]")
            raise typer.Exit(1)

    # Process each file
    results = {}
    for idx, dtsx_file in enumerate(dtsx_files, 1):
        separator = "=" * 60
        console.print(f"\n[bold]{separator}[/bold]")
        console.print(f"[bold cyan]  [{idx}/{len(dtsx_files)}] Processing: {dtsx_file.name}[/bold cyan]")
        console.print(f"[bold]{separator}[/bold]")

        success = _convert_single_file(
            input_file=dtsx_file,
            output_dir=output_dir,
            provider=provider,
            verbose=verbose,
            llm=llm,
        )
        results[dtsx_file.name] = success

    # Batch summary
    if len(dtsx_files) > 1:
        console.print(f"\n\n{'=' * 60}")
        console.print("[bold blue]  BATCH CONVERSION SUMMARY[/bold blue]")
        console.print(f"{'=' * 60}\n")

        table = Table(show_header=True)
        table.add_column("#", justify="right", style="dim")
        table.add_column("Package", style="cyan")
        table.add_column("Status", justify="center")

        for idx, (name, success) in enumerate(results.items(), 1):
            status = "[green]✓ Success[/green]" if success else "[red]✗ Failed[/red]"
            table.add_row(str(idx), name, status)

        console.print(table)

        passed = sum(1 for s in results.values() if s)
        failed = sum(1 for s in results.values() if not s)
        console.print(f"\n  [green]{passed} succeeded[/green], [red]{failed} failed[/red] out of {len(results)} packages")
        console.print(f"  Output: {Path(output_dir).absolute()}\n")

        if failed > 0:
            raise typer.Exit(1)
    else:
        if not all(results.values()):
            raise typer.Exit(1)


@app.command()
def analyze(
    input_file: Path = typer.Argument(..., help="Path to SSIS package file (.dtsx)"),
):
    """
    Analyze an SSIS package and show what will be converted.
    """
    console.print("\n[bold blue]Analyze: SSIS Package Analyzer[/bold blue]\n")

    if not input_file.exists():
        console.print(f"[red]✗ Error: Input file not found: {input_file}[/red]")
        raise typer.Exit(1)

    parser = SSISParser()
    workflow = parser.parse(input_file)

    console.print(f"\n[bold]Workflow:[/bold] {workflow.name}")

    # Mappings
    console.print(f"\n[bold]Mappings ({len(workflow.mappings)}):[/bold]")
    for mapping in workflow.mappings:
        console.print(f"  * {mapping.name}")

    # Transformations
    console.print(f"\n[bold]Transformations ({len(workflow.transformations)}):[/bold]")
    for tx in workflow.transformations:
        console.print(f"  * [cyan][{tx.type}][/cyan] {tx.name}")

    # Sources
    if workflow.sources:
        console.print(f"\n[bold]Sources:[/bold]")
        for src in workflow.sources:
            console.print(f"  * {src}")

    # Targets
    if workflow.targets:
        console.print(f"\n[bold]Targets:[/bold]")
        for tgt in workflow.targets:
            console.print(f"  * {tgt}")


@app.command()
def document(
    input_file: Path = typer.Argument(..., help="Path to SSIS package file (.dtsx)"),
    output_dir: Path = typer.Option("./docs", "--output", "-o", help="Output directory for documentation"),
    provider: Optional[str] = typer.Option(None, "--provider", "-p", help="LLM provider (ollama, gemini, anthropic)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed progress"),
):
    """
    Generate business logic documentation from an SSIS package.

    Creates human-readable documentation explaining the package's business logic,
    transformations, and data flow in plain text format.
    """
    logger = get_logger("document")

    console.print("\n[bold blue]Doc: Business Logic Documentation Generator[/bold blue]\n")

    # Validate input file
    if not input_file.exists():
        console.print(f"[red]✗ Error: Input file not found: {input_file}[/red]")
        raise typer.Exit(1)

    # Create project-specific output directory
    project_name = input_file.stem
    project_dir = output_dir / project_name
    doc_dir = project_dir / "doc"
    doc_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Step 1: Parse SSIS Package
        console.print("\n[yellow]Step 1:[/yellow] Parsing SSIS package...")
        parser = SSISParser()
        workflow = parser.parse(input_file)

        # Use filename stem if workflow name is generic
        if workflow.name in ("Imported Workflow", "Parsed Workflow", "Unknown", ""):
            workflow.name = project_name

        console.print(f"  [green]✓[/green] Found {len(workflow.mappings)} mapping(s)")
        console.print(f"  [green]✓[/green] Found {len(workflow.transformations)} transformation(s)")

        # Step 2: Initialize LLM provider
        console.print("\n[yellow]Step 2:[/yellow] Initializing LLM provider...")
        llm = create_provider(provider=provider)

        # Health check
        if not llm.health_check():
            console.print(f"  [red]✗ Provider {llm.provider_name} is not available[/red]")

            if llm.provider_name == "ollama":
                console.print("\n[yellow]Hint:[/yellow] Make sure Ollama is running:")
                console.print("  1. Install Ollama: https://ollama.ai/download")
                console.print("  2. Start Ollama: ollama serve")
                console.print("  3. Pull model: ollama pull llama3.2")

            raise typer.Exit(1)

        console.print(f"  [green]✓[/green] Using {llm.provider_name} ({llm.model})")

        # Step 3: Generate documentation
        console.print("\n[yellow]Step 3:[/yellow] Generating business logic documentation...")
        generator = BusinessLogicDocGenerator(llm_provider=llm, verbose=verbose)

        result = generator.generate(workflow, doc_dir)

        if result.errors:
            for error in result.errors:
                console.print(f"  [red]Error: {error}[/red]")

        console.print(f"  [green]OK[/green] Generated {len(result.files)} file(s)")

        # Summary
        if result.success:
            console.print("\n[green]OK: Documentation complete![/green]")
        else:
            console.print("\n[yellow]Warning: Documentation completed with errors[/yellow]")

        console.print(f"\n[bold]Output directory:[/bold] {doc_dir.absolute()}")
        for f in result.files:
            console.print(f"  * {Path(f).name}")

        console.print("\n[bold]Next steps:[/bold]")
        console.print("  1. Review the generated documentation")
        console.print("  2. Validate business logic with stakeholders")
        console.print(f"  3. Run conversion: uv run ssis2spark convert {input_file}")

    except Exception as e:
        logger.exception("Documentation generation failed")
        console.print(f"\n[red]✗ Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def providers():
    """
    List available LLM providers and their status.
    """
    console.print("\n[bold blue]LLM Providers[/bold blue]\n")

    config = get_config()
    available = get_available_providers()

    table = Table(show_header=True)
    table.add_column("Provider", style="cyan")
    table.add_column("Model")
    table.add_column("Status")
    table.add_column("Default", justify="center")

    providers_info = [
        ("ollama", config.llm.ollama.model, "ollama" in available),
        ("gemini", config.llm.gemini.model, "gemini" in available),
        ("anthropic", config.llm.anthropic.model, "anthropic" in available),
        ("azure_openai", config.llm.azure_openai.deployment_name, "azure_openai" in available),
    ]

    for name, model, is_available in providers_info:
        status = "[green]Available[/green]" if is_available else "[red]Not configured[/red]"
        is_default = "✓" if name == config.llm.provider else ""
        table.add_row(name, model, status, is_default)

    console.print(table)

    # Show health check for default provider
    console.print(f"\n[bold]Health Check ({config.llm.provider}):[/bold]")
    try:
        llm = create_provider()
        if llm.health_check():
            console.print(f"  [green]OK: Provider is ready[/green]")
        else:
            console.print(f"  [red]Error: Provider not responding[/red]")
    except Exception as e:
        console.print(f"  [red]Error: {e}[/red]")


@app.command(name="list-templates")
def list_templates():
    """
    List available transformation templates.
    """
    console.print("\n[bold blue]Transformation Templates[/bold blue]\n")

    config = get_config()
    templates_dir = Path(config.paths.prompts) / "templates"

    if not templates_dir.exists():
        # Try relative to script
        templates_dir = Path(__file__).parent.parent / "prompts" / "templates"

    if templates_dir.exists():
        templates = sorted(templates_dir.glob("*.md"))
        for template in templates:
            name = template.stem.replace("_", " ").title()
            console.print(f"  * {name}")
    else:
        console.print("[yellow]No templates found.[/yellow]")


@app.command()
def version():
    """
    Show version information.
    """
    config = get_config()
    console.print(f"{config.app.name} v{config.app.version}")


if __name__ == "__main__":
    app()
