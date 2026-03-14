"""
LLM Provider Interface.

Defines the abstract base class that all LLM providers must implement.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class LLMResponse:
    """Response from an LLM provider."""
    text: str
    model: str
    provider: str
    tokens_used: Optional[int] = None
    latency_ms: Optional[float] = None


class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    def __init__(self, model: str, temperature: float = 0.0, max_tokens: int = 8192):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self._prompts_dir: Optional[Path] = None
        self._knowledge_dir: Optional[Path] = None
    
    def set_paths(self, prompts_dir: Path, knowledge_dir: Path) -> None:
        """Set paths for prompts and knowledge files."""
        self._prompts_dir = prompts_dir
        self._knowledge_dir = knowledge_dir
    
    def _load_file(self, directory: Optional[Path], filename: str) -> str:
        """Load a file from the given directory."""
        if directory is None:
            return ""
        path = directory / filename
        if path.exists():
            return path.read_text(encoding='utf-8')
        return ""
    
    def load_prompt(self, filename: str) -> str:
        """Load a prompt file."""
        return self._load_file(self._prompts_dir, filename)
    
    def load_template(self, transformation_type: str) -> str:
        """Load a transformation template."""
        # Name alias table: SSIS component type → actual filename stem
        _ALIASES = {
            "aggregate":     "aggregator",
            "unionall":      "union",
            "conditionalsplit": "conditionalsplit",
            "sort":          "sorter",
            "script":        "script_component",
            "scriptcomponent": "script_component",
            "mergejoin":     "mergejoin",
            "oledbcommand":  "oledb_command",
            "dataconvert":   "data_conversion",
            "derivedcolumn": "expression",
        }
        base = transformation_type.lower().replace(" ", "_")
        stem = _ALIASES.get(base, base)
        filename = f"{stem}.md"

        if self._knowledge_dir:
            # Primary: prompts/knowledge/ssis_reference/data_flow/transformations/<stem>.md
            deep_path = (
                self._knowledge_dir
                / "ssis_reference"
                / "data_flow"
                / "transformations"
                / filename
            )
            if deep_path.exists():
                return deep_path.read_text(encoding="utf-8")
            # Fallback: prompts/knowledge/transformations/<stem>.md  (legacy)
            legacy = self._knowledge_dir / "transformations" / filename
            if legacy.exists():
                return legacy.read_text(encoding="utf-8")
        return ""
    
    def load_knowledge(self, filename: str) -> str:
        """Load a knowledge file, supporting subdirectory paths.

        Args:
            filename: Relative path from the knowledge directory.
                      May use forward slashes for subdirs:
                        'ssis_reference/control_flow/rules.md'
                      Or parent-relative paths:
                        '../transformation_rules.md'
        """
        if not self._knowledge_dir:
            return ""
        # Support POSIX-style relative paths by resolving against the knowledge dir
        try:
            target = (self._knowledge_dir / filename).resolve()
            if target.exists():
                return target.read_text(encoding="utf-8")
        except Exception:
            pass
        return ""
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return the provider name."""
        pass
    
    @abstractmethod
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> LLMResponse:
        """
        Generate a response from the LLM.
        
        Args:
            prompt: The user prompt
            system_prompt: Optional system prompt
            
        Returns:
            LLMResponse with generated text
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if the provider is available.
        
        Returns:
            True if provider is healthy
        """
        pass
    
    def extract_code(self, response: str) -> str:
        """Extract code from LLM response."""
        if "```python" in response:
            start = response.find("```python") + len("```python")
            end = response.find("```", start)
            if end > start:
                return response[start:end].strip()
        
        if "```" in response:
            start = response.find("```") + 3
            newline = response.find("\n", start)
            if newline > start and newline - start < 20:
                start = newline + 1
            end = response.find("```", start)
            if end > start:
                return response[start:end].strip()
        
        return response.strip()
