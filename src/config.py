"""
Configuration management using Pydantic Settings.

Loads configuration from:
1. config/default.yaml (base config)
2. config/local.yaml (local overrides, git-ignored)
3. Environment variables (highest priority)
"""
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class OllamaConfig(BaseModel):
    """Ollama provider configuration."""
    model: str = "qwen2.5-coder:7b"
    base_url: str = "http://localhost:11434"
    timeout: int = 500


class GeminiConfig(BaseModel):
    """Google Gemini provider configuration."""
    model: str = "gemini-2.5-flash-lite"
    timeout: int = 60


class AnthropicConfig(BaseModel):
    """Anthropic Claude provider configuration."""
    model: str = "claude-sonnet-4-20250514"
    timeout: int = 60


class AzureOpenAIConfig(BaseModel):
    """Azure OpenAI provider configuration."""
    deployment_name: str = "gpt-4o-mini"
    endpoint: str = "https://mit-eus-aiops-training-resource.openai.azure.com/"
    api_version: str = "2024-02-15-preview"
    use_responses_api: bool = False
    timeout: int = 60


class AzureOpenAICodexConfig(BaseModel):
    """Azure OpenAI Codex provider configuration."""
    deployment_name: str = "gpt-5.3-codex"
    endpoint: str = "https://mit-eus-jhp-poc-foundry01.cognitiveservices.azure.com/"
    api_version: str = "2025-04-01-preview"
    use_responses_api: bool = True
    timeout: int = 300


class AzureAnthropicConfig(BaseModel):
    """Azure AI Foundry-hosted Anthropic Claude configuration."""
    deployment_name: str = "claude-sonnet-4-6"
    endpoint: str = "https://mit-eus-jhp-poc-foundry01.services.ai.azure.com/anthropic"
    api_version: str = "2024-06-01"
    timeout: int = 300


class DatabricksConfig(BaseModel):
    """Databricks Model Serving provider configuration."""
    model: str = "databricks-meta-llama-3-1-70b-instruct"
    workspace_url: str = ""
    timeout: int = 60


class GenerationConfig(BaseModel):
    """LLM generation settings."""
    temperature: float = 0.0
    max_tokens: int = 8192


class LLMConfig(BaseModel):
    """LLM configuration."""
    provider: Literal["ollama", "gemini", "anthropic", "azure_openai", "azure_openai_codex", "databricks", "azure_anthropic"] = "ollama"
    ollama: OllamaConfig = Field(default_factory=OllamaConfig)
    gemini: GeminiConfig = Field(default_factory=GeminiConfig)
    anthropic: AnthropicConfig = Field(default_factory=AnthropicConfig)
    azure_openai: AzureOpenAIConfig = Field(default_factory=AzureOpenAIConfig)
    azure_openai_codex: AzureOpenAICodexConfig = Field(default_factory=AzureOpenAICodexConfig)
    azure_anthropic: AzureAnthropicConfig = Field(default_factory=AzureAnthropicConfig)
    databricks: DatabricksConfig = Field(default_factory=DatabricksConfig)
    generation: GenerationConfig = Field(default_factory=GenerationConfig)


class LogFileConfig(BaseModel):
    """Log file configuration."""
    enabled: bool = True
    path: str = "logs/app.log"
    rotation: str = "10 MB"
    retention: int = 10


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["json", "console"] = "console"
    file: LogFileConfig = Field(default_factory=LogFileConfig)


class PathsConfig(BaseModel):
    """Path configuration."""
    prompts: str = "prompts"
    knowledge: str = "prompts/knowledge"
    samples: str = "samples"
    output: str = "output"
    golden: str = "golden"


class ConversionConfig(BaseModel):
    """Conversion settings."""
    include_comments: bool = True
    include_type_hints: bool = True
    output_style: Literal["single_file", "multi_file"] = "single_file"


class AppConfig(BaseModel):
    """Application metadata."""
    name: str = "ssis-to-pyspark"
    version: str = "0.1.0"
    environment: Literal["development", "staging", "production"] = "development"


class Settings(BaseSettings):
    """
    Main settings class.
    
    Configuration priority (highest to lowest):
    1. Environment variables
    2. config/local.yaml
    3. config/default.yaml
    """
    app: AppConfig = Field(default_factory=AppConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    conversion: ConversionConfig = Field(default_factory=ConversionConfig)
    
    # API Keys (from environment)
    google_api_key: Optional[str] = Field(default=None, alias="GOOGLE_API_KEY")
    anthropic_api_key: Optional[str] = Field(default=None, alias="ANTHROPIC_API_KEY")
    azure_openai_api_key: Optional[str] = Field(default=None, alias="AZURE_OPENAI_API_KEY")
    azure_openai_codex_api_key: Optional[str] = Field(default=None, alias="AZURE_OPENAI_CODEX_API_KEY")
    azure_anthropic_api_key: Optional[str] = Field(default=None, alias="AZURE_ANTHROPIC_API_KEY")
    databricks_token: Optional[str] = Field(default=None, alias="DATABRICKS_TOKEN")
    databricks_workspace_url: Optional[str] = Field(default=None, alias="DATABRICKS_WORKSPACE_URL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


def load_yaml_config(path: Path) -> dict:
    """Load configuration from YAML file."""
    if path.exists():
        with open(path, encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    return {}


def get_settings() -> Settings:
    """
    Load and merge settings from all sources.
    
    Returns:
        Settings: Merged configuration object
    """
    # Find project root (where pyproject.toml is)
    current = Path(__file__).parent.parent
    config_dir = current / "config"
    
    # Load YAML configs
    default_config = load_yaml_config(config_dir / "default.yaml")
    local_config = load_yaml_config(config_dir / "local.yaml")
    
    # Merge configs (local overrides default)
    merged = {**default_config}
    for key, value in local_config.items():
        if isinstance(value, dict) and key in merged:
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    
    # Create settings (env vars will override)
    return Settings(**merged)


# Singleton instance
_settings: Optional[Settings] = None


def get_config() -> Settings:
    """Get cached settings instance."""
    global _settings
    if _settings is None:
        _settings = get_settings()
    return _settings
