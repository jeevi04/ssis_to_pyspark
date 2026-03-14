"""
LLM Provider Factory.

Creates the appropriate LLM provider based on configuration.
"""
from pathlib import Path
from typing import Literal, Optional

from src.config import Settings, get_config
from src.llm.base import BaseLLMProvider
from src.logging import get_logger

logger = get_logger(__name__)

ProviderType = Literal["ollama", "gemini", "anthropic", "azure_openai", "azure_openai_codex", "databricks", "azure_anthropic"]


def create_provider(
    provider: Optional[ProviderType] = None,
    config: Optional[Settings] = None,
) -> BaseLLMProvider:
    """
    Create an LLM provider based on configuration.
    
    Args:
        provider: Override provider type (ollama, gemini, anthropic)
        config: Override settings
        
    Returns:
        Configured LLM provider instance
    """
    config = config or get_config()
    provider = provider or config.llm.provider
    
    logger.info("Creating LLM provider", provider=provider)
    
    # Find project root for paths
    project_root = Path(__file__).parent.parent.parent
    prompts_dir = project_root / config.paths.prompts
    knowledge_dir = project_root / config.paths.knowledge
    
    if provider == "ollama":
        from src.llm.ollama_provider import OllamaProvider
        
        llm = OllamaProvider(
            model=config.llm.ollama.model,
            base_url=config.llm.ollama.base_url,
            temperature=config.llm.generation.temperature,
            max_tokens=config.llm.generation.max_tokens,
            timeout=config.llm.ollama.timeout,
        )
    
    elif provider == "gemini":
        from src.llm.gemini_provider import GeminiProvider
        
        if not config.google_api_key:
            raise ValueError(
                "GOOGLE_API_KEY environment variable not set. "
                "Please set it in your .env file."
            )
        
        llm = GeminiProvider(
            model=config.llm.gemini.model,
            api_key=config.google_api_key,
            temperature=config.llm.generation.temperature,
            max_tokens=config.llm.generation.max_tokens,
            timeout=config.llm.gemini.timeout,
        )
    
    elif provider == "anthropic":
        from src.llm.anthropic_provider import AnthropicProvider
        
        if not config.anthropic_api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY environment variable not set. "
                "Please set it in your .env file."
            )
        
        llm = AnthropicProvider(
            model=config.llm.anthropic.model,
            api_key=config.anthropic_api_key,
            temperature=config.llm.generation.temperature,
            max_tokens=config.llm.generation.max_tokens,
            timeout=config.llm.anthropic.timeout,
        )
    
    elif provider == "azure_openai":
        from src.llm.azure_openai_provider import AzureOpenAIProvider

        llm = AzureOpenAIProvider(
            deployment_name=config.llm.azure_openai.deployment_name,
            endpoint=config.llm.azure_openai.endpoint,
            api_key=config.azure_openai_api_key,
            api_version=config.llm.azure_openai.api_version,
            temperature=config.llm.generation.temperature,
            max_tokens=config.llm.generation.max_tokens,
            use_responses_api=config.llm.azure_openai.use_responses_api,
            timeout=config.llm.azure_openai.timeout,
        )

    elif provider == "azure_openai_codex":
        from src.llm.azure_openai_provider import AzureOpenAIProvider

        if not config.azure_openai_codex_api_key:
            raise ValueError(
                "AZURE_OPENAI_CODEX_API_KEY environment variable not set. "
                "Please set it in your .env file."
            )

        llm = AzureOpenAIProvider(
            deployment_name=config.llm.azure_openai_codex.deployment_name,
            endpoint=config.llm.azure_openai_codex.endpoint,
            api_key=config.azure_openai_codex_api_key,
            api_version=config.llm.azure_openai_codex.api_version,
            temperature=config.llm.generation.temperature,
            max_tokens=config.llm.generation.max_tokens,
            use_responses_api=config.llm.azure_openai_codex.use_responses_api,
            timeout=config.llm.azure_openai_codex.timeout,
        )
    
    elif provider == "databricks":
        from src.llm.databricks_provider import DatabricksProvider
        
        if not config.databricks_token:
            raise ValueError(
                "DATABRICKS_TOKEN environment variable not set. "
                "Please set it in your .env file."
            )
        
        llm = DatabricksProvider(
            model=config.llm.databricks.model,
            workspace_url=config.llm.databricks.workspace_url or config.databricks_workspace_url,
            api_token=config.databricks_token,
            temperature=config.llm.generation.temperature,
            max_tokens=config.llm.generation.max_tokens,
            timeout=config.llm.databricks.timeout,
        )
    
    elif provider == "azure_anthropic":
        from src.llm.azure_anthropic_provider import AzureAnthropicProvider

        if not config.azure_anthropic_api_key:
            raise ValueError(
                "AZURE_ANTHROPIC_API_KEY environment variable not set. "
                "Please set it in your .env file."
            )

        llm = AzureAnthropicProvider(
            deployment_name=config.llm.azure_anthropic.deployment_name,
            endpoint=config.llm.azure_anthropic.endpoint,
            api_key=config.azure_anthropic_api_key,
            api_version=config.llm.azure_anthropic.api_version,
            temperature=config.llm.generation.temperature,
            max_tokens=config.llm.generation.max_tokens,
            timeout=config.llm.azure_anthropic.timeout,
        )

    else:
        raise ValueError(f"Unknown provider: {provider}")
    
    # Set paths for prompts and knowledge
    llm.set_paths(prompts_dir, knowledge_dir)
    
    return llm


def get_available_providers(config: Optional[Settings] = None) -> list[str]:
    """Get list of available (configured) providers."""
    config = config or get_config()
    available = ["ollama"]  # Ollama is always available locally
    
    if config.google_api_key:
        available.append("gemini")
    
    if config.anthropic_api_key:
        available.append("anthropic")
    
    if config.azure_openai_api_key:
        available.append("azure_openai")

    if config.azure_openai_codex_api_key:
        available.append("azure_openai_codex")

    if config.azure_anthropic_api_key:
        available.append("azure_anthropic")

    if config.databricks_token:
        available.append("databricks")

    return available
