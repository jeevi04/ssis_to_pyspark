"""
Ollama LLM Provider.

Connects to locally running Ollama instance for LLM inference.
"""
import time
from typing import Optional

import ollama
from ollama import Client, ResponseError

from src.llm.base import BaseLLMProvider, LLMResponse
from src.logging import get_logger

logger = get_logger(__name__)


class OllamaProvider(BaseLLMProvider):
    """Ollama local LLM provider."""
    
    def __init__(
        self,
        model: str = "qwen2.5-coder:7b",
        base_url: str = "http://localhost:11434",
        temperature: float = 0.0,
        max_tokens: int = 8192,
        timeout: int = 500,
    ):
        super().__init__(model, temperature, max_tokens)
        self.base_url = base_url
        self.timeout = timeout
        self.client = Client(host=base_url, timeout=timeout)
        
        logger.info(
            "Initialized Ollama provider",
            model=model,
            base_url=base_url,
        )
    
    @property
    def provider_name(self) -> str:
        return "ollama"
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> LLMResponse:
        """Generate response using Ollama."""
        start_time = time.time()
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        logger.debug(
            "Sending request to Ollama",
            model=self.model,
            prompt_length=len(prompt),
        )
        
        try:
            response = self.client.chat(
                model=self.model,
                messages=messages,
                options={
                    "temperature": self.temperature,
                    "num_predict": self.max_tokens,
                },
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            # Extract token counts if available
            tokens_used = None
            if "eval_count" in response:
                tokens_used = response.get("prompt_eval_count", 0) + response.get("eval_count", 0)
            
            logger.info(
                "Ollama response received",
                model=self.model,
                latency_ms=round(latency_ms, 2),
                tokens=tokens_used,
            )
            
            return LLMResponse(
                text=response["message"]["content"],
                model=self.model,
                provider=self.provider_name,
                tokens_used=tokens_used,
                latency_ms=latency_ms,
            )
            
        except ResponseError as e:
            logger.error("Ollama API error", error=str(e))
            raise
        except Exception as e:
            logger.error("Ollama request failed", error=str(e))
            raise
    
    def health_check(self) -> bool:
        """Check if Ollama is running and model is available."""
        try:
            # List models to check connection
            models = self.client.list()
            
            # Handle both dict and object response formats
            model_list = models.get("models", []) if isinstance(models, dict) else getattr(models, "models", [])
            
            model_names = []
            for m in model_list:
                if isinstance(m, dict):
                    name = m.get("name", "").split(":")[0]
                else:
                    name = getattr(m, "model", "").split(":")[0]
                model_names.append(name)
            
            target_model = self.model.split(":")[0]
            
            if target_model not in model_names:
                logger.warning(
                    "Model not found in Ollama",
                    model=self.model,
                    available=model_names,
                )
                return False
            
            logger.debug("Ollama health check passed", model=self.model)
            return True
            
        except Exception as e:
            logger.error("Ollama health check failed", error=str(e))
            return False
    
    def pull_model(self) -> bool:
        """Pull the model if not available."""
        try:
            logger.info("Pulling model from Ollama", model=self.model)
            self.client.pull(self.model)
            logger.info("Model pulled successfully", model=self.model)
            return True
        except Exception as e:
            logger.error("Failed to pull model", model=self.model, error=str(e))
            return False
    
    def list_models(self) -> list[str]:
        """List available models."""
        try:
            models = self.client.list()
            model_list = models.get("models", []) if isinstance(models, dict) else getattr(models, "models", [])
            
            result = []
            for m in model_list:
                if isinstance(m, dict):
                    result.append(m.get("name", ""))
                else:
                    result.append(getattr(m, "model", ""))
            return result
        except Exception:
            return []
