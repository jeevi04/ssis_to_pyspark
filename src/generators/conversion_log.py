"""
Conversion Log / KT Document Generator for PySpark.

Generates a Knowledge Transfer (KT) document summarizing the conversion,
optimizations, and areas for developer review.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from src.config import get_config
from src.llm import BaseLLMProvider
from src.logging import get_logger, LogContext
from src.parsers import Workflow

logger = get_logger(__name__)

@dataclass
class LogGenerationResult:
    """Result of conversion log generation."""
    success: bool
    files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

class ConversionLogGenerator:
    """
    Generates a conversion log / KT document.
    """
    
    def __init__(self, llm_provider: BaseLLMProvider, verbose: bool = False):
        self.llm = llm_provider
        self.verbose = verbose
        self.config = get_config()
        
        # Load documentation-specific prompt if available, fallback to system prompt
        try:
            self.system_prompt = self.llm.load_prompt("system.md")
        except:
            self.system_prompt = "You are an expert data engineer documenting an ETL migration."

    def generate(self, workflow: Workflow, metadata: list[dict], output_dir: Path) -> LogGenerationResult:
        """
        Generate the conversion log.
        
        Args:
            workflow: Parsed SSIS package
            metadata: Metadata collected during PySpark generation
            output_dir: Directory to write the log
            
        Returns:
            LogGenerationResult with file paths and any errors
        """
        result = LogGenerationResult(success=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with LogContext(workflow=workflow.name):
            logger.info("Generating conversion log", workflow=workflow.name)
            
            if not metadata:
                logger.warning("No metadata provided for conversion log")
                metadata_str = "No specific conversion notes were captured for this workflow."
            else:
                # Format the metadata for the prompt
                metadata_str = ""
                for item in metadata:
                    metadata_str += f"### Mapping: {item['mapping']} | Transformation: {item['transformation']} ({item['type']})\n"
                    metadata_str += f"{item['notes']}\n\n"
            
            prompt = f"""
## Task: Generate Conversion Log / KT Document

You are creating a Knowledge Transfer (KT) document for a developer who will take over the converted PySpark code. 
The conversion was done from SSIS (SQL Server Integration Services) to PySpark using an AI-powered tool.

### Workflow Details:
- **Workflow Name:** {workflow.name}
- **Mappings:** {len(workflow.mappings)}
- **Sources:** {', '.join([s.name for s in workflow.sources]) if workflow.sources else 'N/A'}
- **Targets:** {', '.join([t.name for t in workflow.targets]) if workflow.targets else 'N/A'}

### AI Conversion Notes (Collected during transformation conversion):
{metadata_str}

## Requirements for the KT Document:
1. **Professional Title**: e.g., "SSIS to PySpark Conversion Log - {workflow.name}"
2. **Executive Summary**: High-level overview of the conversion. Mention that this is an AI-assisted conversion using PySpark DataFrame API and requires human review.
3. **Optimizations Applied**: Consolidated list of performance or logic improvements made by the AI (e.g., using broadcast joins, vectorizing logic, leveraging Spark native functions).
4. **Developer Action Items / Review Notes**: List specific areas that require manual review, potential edge cases, or complex logic that needs verification.
5. **Technical Nuances**: Mention any mapping/transformation types that were particularly complex or used custom utility functions from `utils.py`.

## Formatting:
- Use clean Markdown with clear headers and bullet points.
- Be professional, concise, and helpful for a fellow data engineer.
- Ensure the tone highlights both what was automated and where human expertise is needed.

Generate the complete Markdown content below.
"""
            
            try:
                response = self.llm.generate(prompt, self.system_prompt)
                filename = f"{self._sanitize_filename(workflow.name)}_conversion_log.md"
                file_path = output_dir / filename
                file_path.write_text(response.text, encoding='utf-8')
                result.files.append(str(file_path))
                logger.info("Conversion log generated", file=str(file_path))
            except Exception as e:
                result.success = False
                result.errors.append(f"Error generating conversion log: {e}")
                logger.error("Failed to generate conversion log", error=str(e))
        
        return result

    def _sanitize_filename(self, name: str) -> str:
        """Convert name to valid filename."""
        sanitized = "".join(c if c.isalnum() else "_" for c in name)
        while "__" in sanitized:
            sanitized = sanitized.replace("__", "_")
        sanitized = sanitized.strip("_").lower()
        if sanitized and sanitized[0].isdigit():
            sanitized = "wf_" + sanitized
        return sanitized
