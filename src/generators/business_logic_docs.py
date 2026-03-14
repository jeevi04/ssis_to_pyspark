"""
Business Logic Documentation Generator.

Generates human-readable documentation from SSIS packages
using LLM to explain business logic, transformations, and data flow.
"""
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from src.config import get_config
from src.llm import BaseLLMProvider
from src.logging import get_logger, LogContext
from src.parsers import Mapping, Transformation, Workflow

logger = get_logger(__name__)


@dataclass
class DocumentationResult:
    """Result of documentation generation."""
    success: bool
    files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class BusinessLogicDocGenerator:
    """
    Generates business logic documentation from SSIS packages.
    
    Creates human-readable documentation that explains:
    - Workflow purpose and overview
    - Data flow in pseudocode
    - Transformation logic and business rules
    - Data quality rules
    """
    
    def __init__(self, llm_provider: BaseLLMProvider, verbose: bool = False):
        self.llm = llm_provider
        self.verbose = verbose
        self.config = get_config()
        
        # Load documentation-specific prompt
        try:
            self.system_prompt = self.llm.load_prompt("document_business_logic.md")
        except:
            # Fallback to generic prompt
            self.system_prompt = """You are a business analyst and technical writer.
Your task is to document SSIS ETL packages in clear, human-readable language.
Focus on explaining the business logic, data transformations, and rules.
Write for a technical audience but avoid implementation details.
Use pseudocode to show data flow."""
    
    def generate(self, workflow: Workflow, output_dir: Path) -> DocumentationResult:
        """
        Generate business logic documentation for a workflow.
        
        Args:
            workflow: Parsed SSIS package
            output_dir: Directory to write documentation
            
        Returns:
            DocumentationResult with file paths and any errors
        """
        result = DocumentationResult(success=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with LogContext(workflow=workflow.name):
            logger.info("Starting business logic documentation", workflow=workflow.name)
            
            # Generate workflow overview (Section 1)
            overview = self._generate_workflow_overview(workflow)
            
            # Get Source and Target details (Sections 2 & 3)
            source_details = self._get_source_details(workflow)
            target_details = self._get_target_details(workflow)
            
            # Generate documentation for each mapping
            mapping_docs = []
            all_transformations = []
            for mapping in workflow.mappings:
                try:
                    doc, txs = self._generate_mapping_documentation(mapping)
                    mapping_docs.append(doc)
                    all_transformations.extend(txs)
                except Exception as e:
                    error_msg = f"Error documenting {mapping.name}: {e}"
                    result.errors.append(error_msg)
                    result.success = False
                    logger.error("Documentation failed", mapping=mapping.name, error=str(e))
            
            # Generate transformation summary and lineage (Sections 4 & 5)
            transformation_summary = self._generate_transformation_summary(workflow, all_transformations)
            lineage = self._generate_data_flow_lineage(workflow)
            
            # Generate pseudocode (Section 6)
            pseudocode = self._generate_pseudocode_flow_workflow(workflow)
            
            # Assemble complete documentation using LLM
            full_doc = self._assemble_documentation(
                workflow, 
                overview, 
                source_details, 
                target_details, 
                transformation_summary, 
                lineage, 
                pseudocode,
                mapping_docs
            )
            
            # Write to file
            filename = self._sanitize_filename(workflow.name) + "_business_logic.md"
            file_path = output_dir / filename
            file_path.write_text(full_doc, encoding='utf-8')
            result.files.append(str(file_path))
            
            logger.info("Documentation complete", file=str(file_path))
        
        return result
    
    def _generate_workflow_overview(self, workflow: Workflow) -> str:
        """Generate high-level workflow overview using LLM."""
        prompt = f"""
Analyze this SSIS package and provide a detailed business-focused overview.

Workflow Name: {workflow.name}
Description: {workflow.description or "N/A"}
Number of Mappings: {len(workflow.mappings)}
Sources: {', '.join([s.name for s in workflow.sources]) if workflow.sources else 'N/A'}
Targets: {', '.join([t.name for t in workflow.targets]) if workflow.targets else 'N/A'}

Provide:
1. High-level explanation of what the workflow accomplishes (2-3 paragraphs)
2. Business purpose and objectives: What problem does this solve?
3. Key business rules being applied throughout the workflow.

Focus on the business perspective. Avoid technical jargon.
"""
        response = self.llm.generate(prompt, self.system_prompt)
        return response.text

    def _get_source_details(self, workflow: Workflow) -> str:
        """Format detailed source information."""
        parts = [f"Total Sources: {len(workflow.sources)}"]
        for i, source in enumerate(workflow.sources, 1):
            key_cols = [f.name for f in source.fields[:10]]
            parts.append(f"""
Source {i}: {source.name}
  - Type: {source.type}
  - Connection: {source.dbdname or "See workflow XML"}
  - Key Attributes: {', '.join(key_cols)}
  - Description: {source.description or "Source data for processing"}""")
        return "\n".join(parts)

    def _get_target_details(self, workflow: Workflow) -> str:
        """Format detailed target information."""
        parts = [f"Total Targets: {len(workflow.targets)}"]
        for i, target in enumerate(workflow.targets, 1):
            cols = [f.name for f in target.fields[:10]]
            parts.append(f"""
Target {i}: {target.name}
  - Type: {target.type}
  - Connection: See workflow XML
  - Output Attributes: {', '.join(cols)}
  - Description: {target.description or "Target destination for processed data"}""")
        return "\n".join(parts)

    def _generate_transformation_summary(self, workflow: Workflow, transformations: list[Transformation]) -> str:
        """Generate summary of transformations."""
        breakdown = {}
        for tx in transformations:
            breakdown[tx.type] = breakdown.get(tx.type, 0) + 1
        
        breakdown_str = "\n".join([f"- {k}: {v}" for k, v in breakdown.items()])
        
        # Select representative transformations for logic explanation
        key_txs = transformations[:30] # Limit to first 30 to allow more detail
        
        tx_briefs = []
        for i, tx in enumerate(key_txs, 1):
            logic_detail = ""
            if tx.type == "Expression" and tx.expression_code:
                logic_detail = f"\n   Expressions: {tx.expression_code[:200]}..."
            elif tx.type in ("Joiner", "Joiner Transformation") and tx.join_condition:
                logic_detail = f"\n   Join Condition: {tx.join_condition}"
            elif tx.type in ("Lookup Procedure", "Lookup") and tx.lookup_condition:
                logic_detail = f"\n   Lookup Condition: {tx.lookup_condition}"
            elif tx.type == "Filter" and tx.filter_condition:
                logic_detail = f"\n   Filter Condition: {tx.filter_condition}"
            
            tx_briefs.append(f"{i}. {tx.name} ({tx.type}){logic_detail}")
        
        prompt = f"""
For each of the following transformations in an SSIS ETL package, provide a 1-sentence conceptual explanation of what it does:

{chr(10).join(tx_briefs)}

Format your response as a numbered list matching the input.
"""
        resp = self.llm.generate(prompt, self.system_prompt)
        tx_logic_summary = resp.text.strip()

        return f"""
Total Transformations: {len(transformations)}

Transformation Type Breakdown:
{breakdown_str}

Key Transformations Logic:

{tx_logic_summary}
"""

    def _generate_data_flow_lineage(self, workflow: Workflow) -> str:
        """Generate visual text representation of data flow."""
        lineage_parts = []
        for mapping in workflow.mappings:
            ordered = mapping.get_execution_order()
            path = []
            for tx in ordered:
                desc = f" ({tx.type})"
                if tx.type == "Expression":
                    desc = " (Expression: Business logic/transformation)"
                elif tx.type == "Filter":
                    desc = f" (Filter: {tx.filter_condition[:50]}...)" if tx.filter_condition else " (Filter)"
                
                path.append(f"    → [{tx.name}]{desc}")
            
            mapping_lineage = f"MAPPING: {mapping.name}\n" + "\n".join(path)
            lineage_parts.append(mapping_lineage)
            
        return "\n\n".join(lineage_parts)

    def _generate_pseudocode_flow_workflow(self, workflow: Workflow) -> str:
        """Generate high-level pseudocode for the workflow."""
        prompt = f"""
Create high-level pseudocode for this SSIS package.
Workflow: {workflow.name}
Mappings: {', '.join([m.name for m in workflow.mappings])}
Sources: {', '.join([s.name for s in workflow.sources])}
Targets: {', '.join([t.name for t in workflow.targets])}

Requirements:
- Short representation (10-20 lines)
- Capture essential logic flow
- Use simple, readable syntax
- Focus on business steps (READ, FILTER, JOIN, AGGREGATE, WRITE)
"""
        response = self.llm.generate(prompt, self.system_prompt)
        return response.text
    
    def _generate_mapping_documentation(self, mapping: Mapping) -> tuple[str, list[Transformation]]:
        """Generate documentation for a single mapping."""
        # Get execution order
        ordered_transformations = mapping.get_execution_order()
        
        # Document each transformation
        transformation_docs = []
        for tx in ordered_transformations:
            if self.verbose:
                logger.debug("Documenting transformation", type=tx.type, name=tx.name)
            
            doc = self._document_transformation(tx)
            transformation_docs.append(doc)
        
        # Assemble mapping documentation
        mapping_doc = f"""
### MAPPING: {mapping.name}
{chr(10).join(transformation_docs)}
"""
        return mapping_doc, ordered_transformations
    
    
    def _document_transformation(self, tx: Transformation) -> str:
        """Document a single transformation using LLM."""
        # Build transformation details
        tx_info = {
            "name": tx.name,
            "type": tx.type,
            "description": tx.description or "",
            "num_fields": len(tx.fields),
            "filter_condition": tx.filter_condition,
            "join_condition": tx.join_condition,
            "lookup_condition": tx.lookup_condition,
            "group_by": tx.group_by,
        }
        
        # Sample key fields (limit to avoid token overflow)
        key_fields = []
        for f in tx.fields[:10]:  # Limit to first 10 fields
            if f.expression and f.port_type == "OUTPUT":
                key_fields.append({
                    "name": f.name,
                    "expression": f.expression,
                    "datatype": f.datatype
                })
        
        prompt = f"""
Document this SSIS transformation in business terms.

Transformation: {tx.name}
Type: {tx.type}
Fields: {len(tx.fields)} total

Key Output Fields (sample):
{json.dumps(key_fields, indent=2) if key_fields else 'N/A'}

Expression Code: {tx.expression_code or 'N/A'}
Filter Condition: {tx.filter_condition or 'N/A'}
Join Condition: {tx.join_condition or 'N/A'}
Lookup Condition: {tx.lookup_condition or 'N/A'}
Group By: {tx.group_by or 'N/A'}

Provide:
1. PURPOSE: What does this transformation do? (1-2 sentences)
2. LOGIC: Explain the key business logic (mid-level detail)
3. BUSINESS RULES: Any business rules or validations applied

Format as Markdown. Be concise but clear.
"""
        
        response = self.llm.generate(prompt, self.system_prompt)
        
        return f"""
{'-' * 80}
{tx.name} ({tx.type})
{'-' * 80}
{response.text}
"""
    
    def _assemble_documentation(
        self,
        workflow: Workflow,
        overview: str,
        source_details: str,
        target_details: str,
        transformation_summary: str,
        lineage: str,
        pseudocode: str,
        mapping_docs: list[str]
    ) -> str:
        """Assemble all documentation into the strictly standard 6-section template."""
        prompt = f"""
## Task: Create In-Depth Business Logic Documentation

You are an expert Data Analyst. Your task is to produce a human-readable, professional document explaining an SSIS ETL package. 
You must strictly follow the template below and ensure the content is "in-depth" and technically accurate yet business-friendly.

### REQUIRED TEMPLATE:
# Business Logic Documentation: {workflow.name}
Original XML: See workflow properties
Analysis Date: [Insert Current Date]

## 1. Business Logic Overview
[Content from Overview section below]

## 2. Source Details
[Content from Source Details below]

## 3. Target Details
[Content from Target Details below]

## 4. Transformations Summary
[Content from Transformation Summary below]

## 5. Data Flow Lineage
[Content from Lineage below]

## 6. High-Level Pseudocode
[Content from Pseudocode below]

---
*End of Document*

### INPUT DATA:

1. OVERVIEW CONTENT:
{overview}

2. SOURCE DETAILS:
{source_details}

3. TARGET DETAILS:
{target_details}

4. TRANSFORMATION SUMMARY:
{transformation_summary}

5. LINEAGE:
{lineage}

6. PSEUDOCODE:
{pseudocode}

7. IN-DEPTH TRANSFORMATION DETAILS (Use this to enrich sections 1 and 4):
{chr(10).join(mapping_docs)}

### GUIDELINES:
- Ensure section 1 is 2-3 paragraphs and provides a deep understanding of the business purpose.
- Ensure section 4 uses the provided summary but is enriched with details from the "In-depth transformation details" if relevant.
- Maintain the visual representation format for section 5.
- Use Markdown headers, bolding, and lists for a professional look.
- Do NOT use ASCII dividers like === or --- for sections; use Markdown horizontal rules or headers instead.
"""
        
        response = self.llm.generate(prompt, self.system_prompt)
        return response.text

    def _build_documentation_assembly_prompt(
        self,
        workflow: Workflow,
        overview: str,
        mapping_docs: list[str],
    ) -> str:
        """Build prompt for LLM to assemble the final documentation."""
        parts = []
        
        parts.append(f"""
## Task: Assemble Complete Business Logic Documentation

You are creating a comprehensive, professional business logic documentation document for an SSIS ETL package that is being migrated to PySpark.

### Workflow Information:
- **Workflow Name:** {workflow.name}
- **Number of Mappings:** {len(workflow.mappings)}
- **Number of Sources:** {len(workflow.sources)}
- **Number of Targets:** {len(workflow.targets)}

### Workflow Overview:
{overview}

### Individual Mapping Documentation:
Below are the detailed documentation sections for each mapping in this workflow. Your task is to assemble them into a cohesive, well-structured document.

{chr(10).join([f"--- Mapping {i+1} ---{chr(10)}{doc}" for i, doc in enumerate(mapping_docs)])}

## Requirements:

1. **Document Structure:**
   - Create a professional header with title, workflow name, and generation metadata
   - Include a table of contents with links to each major section
   - Organize content in a logical hierarchy with clear section headers
   - Use consistent formatting throughout (headers, dividers, spacing)

2. **Executive Summary Section:**
   - Synthesize the workflow overview into a compelling executive summary
   - Highlight the key business value and objectives
   - Summarize the overall data flow at a high level
   - Mention critical business rules or transformations

3. **Workflow Architecture Section:**
   - Describe the overall workflow structure
   - List all source systems and target destinations
   - Provide a high-level data lineage description
   - Explain how mappings relate to each other (if applicable)

4. **Detailed Mapping Documentation:**
   - Present each mapping's documentation clearly
   - Ensure consistent formatting across all mappings
   - Add transitional text between mappings to improve flow
   - Highlight dependencies or relationships between mappings

5. **Business Rules Summary:**
   - Create a consolidated section listing all key business rules
   - Extract business rules from individual transformations
   - Group related rules together
   - Make them easily scannable

6. **Technical Notes Section:**
   - Document any technical considerations
   - Note complex transformations that require special attention
   - Highlight areas that may need manual review during migration

7. **Appendix (Optional):**
   - Data dictionaries or field mappings if relevant
   - Glossary of business terms
   - References to related systems or documentation

8. **Professional Formatting:**
   - Use clear section dividers (using ===, ---, or similar)
   - Maintain consistent indentation and spacing
   - Use bullet points and numbered lists appropriately
   - Ensure readability with proper line breaks and whitespace

9. **Tone and Style:**
   - Write for a technical business audience (data engineers, analysts, business users)
   - Be clear and concise but thorough
   - Avoid overly technical implementation details
   - Focus on WHAT the workflow does and WHY, not just HOW

## Output:
Generate a complete, professional business logic documentation document that is ready to be shared with stakeholders. The document should be comprehensive yet readable, serving as both a reference guide and a migration planning tool.

The documentation should be in Markdown format with clear formatting.
""")
        
        return "\n\n".join(parts)
    
    def _sanitize_filename(self, name: str) -> str:
        """Convert name to valid filename."""
        sanitized = "".join(c if c.isalnum() else "_" for c in name)
        while "__" in sanitized:
            sanitized = sanitized.replace("__", "_")
        sanitized = sanitized.strip("_").lower()
        if sanitized and sanitized[0].isdigit():
            sanitized = "wf_" + sanitized
        return sanitized