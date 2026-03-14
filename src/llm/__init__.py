"""
LLM Providers Package.

Supports multiple LLM backends:
- Ollama (local, recommended for development)
- Google Gemini
- Anthropic Claude (direct API)
- Azure OpenAI (GPT-4o etc.)
- Azure OpenAI Codex (gpt-5.3-codex)
- Azure Anthropic Claude (Azure AI Foundry)
- Databricks Model Serving
"""
from src.llm.base import BaseLLMProvider, LLMResponse
from src.llm.factory import create_provider, get_available_providers

__all__ = [
    "BaseLLMProvider",
    "LLMResponse",
    "create_provider",
    "get_available_providers",
]
