"""
Anthropic Claude LLM Provider.

Connects to Anthropic's Claude API for LLM inference.
"""
import time
from typing import Optional

from src.llm.base import BaseLLMProvider, LLMResponse
from src.logging import get_logger

logger = get_logger(__name__)


class AnthropicProvider(BaseLLMProvider):
    """Anthropic Claude LLM provider."""
    
    def __init__(
        self,
        model: str = "claude-sonnet-4-20250514",
        api_key: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 8192,
        timeout: int = 60,
    ):
        super().__init__(model, temperature, max_tokens)
        self.timeout = timeout
        
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is required for Anthropic provider")
        
        # Import here to avoid loading if not used
        from anthropic import Anthropic
        
        self.client = Anthropic(api_key=api_key, timeout=timeout)
        
        logger.info("Initialized Anthropic provider", model=model)
    
    @property
    def provider_name(self) -> str:
        return "anthropic"
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> LLMResponse:
        """Generate response using Claude."""
        start_time = time.time()
        
        logger.debug(
            "Sending request to Anthropic",
            model=self.model,
            prompt_length=len(prompt),
        )
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                system=system_prompt or "",
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            # Get token usage
            tokens_used = None
            if response.usage:
                tokens_used = response.usage.input_tokens + response.usage.output_tokens
            
            logger.info(
                "Anthropic response received",
                model=self.model,
                latency_ms=round(latency_ms, 2),
                tokens=tokens_used,
            )
            
            return LLMResponse(
                text=response.content[0].text,
                model=self.model,
                provider=self.provider_name,
                tokens_used=tokens_used,
                latency_ms=latency_ms,
            )
            
        except Exception as e:
            logger.error("Anthropic request failed", error=str(e))
            raise
    
    def health_check(self) -> bool:
        """Check if Anthropic API is accessible."""
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=10,
                messages=[{"role": "user", "content": "Say 'ok'"}],
            )
            return len(response.content[0].text) > 0
        except Exception as e:
            logger.error("Anthropic health check failed", error=str(e))
            return False
