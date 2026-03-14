"""
Google Gemini LLM Provider.

Connects to Google's Gemini API for LLM inference.
"""
import time
from typing import Optional

from src.llm.base import BaseLLMProvider, LLMResponse
from src.logging import get_logger

logger = get_logger(__name__)


class GeminiProvider(BaseLLMProvider):
    """Google Gemini LLM provider."""
    
    def __init__(
        self,
        model: str = "gemini-2.5-flash-lite",
        api_key: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 8192,
        timeout: int = 60,
    ):
        super().__init__(model, temperature, max_tokens)
        self.timeout = timeout
        
        if not api_key:
            raise ValueError("GOOGLE_API_KEY is required for Gemini provider")
        
        # Import here to avoid loading if not used
        import google.generativeai as genai
        
        genai.configure(api_key=api_key)
        self.client = genai.GenerativeModel(
            model_name=model,
            generation_config={
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            }
        )
        self._genai = genai
        
        logger.info("Initialized Gemini provider", model=model)
    
    @property
    def provider_name(self) -> str:
        return "gemini"
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> LLMResponse:
        """Generate response using Gemini."""
        start_time = time.time()
        
        # Combine system prompt with user prompt
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"""<system>
{system_prompt}
</system>

{prompt}"""
        
        logger.debug(
            "Sending request to Gemini",
            model=self.model,
            prompt_length=len(full_prompt),
        )
        
        try:
            response = self.client.generate_content(full_prompt)
            
            latency_ms = (time.time() - start_time) * 1000
            
            logger.info(
                "Gemini response received",
                model=self.model,
                latency_ms=round(latency_ms, 2),
            )
            
            return LLMResponse(
                text=response.text,
                model=self.model,
                provider=self.provider_name,
                latency_ms=latency_ms,
            )
            
        except Exception as e:
            logger.error("Gemini request failed", error=str(e))
            raise
    
    def health_check(self) -> bool:
        """Check if Gemini API is accessible."""
        try:
            # Simple test generation
            response = self.client.generate_content("Say 'ok'")
            return len(response.text) > 0
        except Exception as e:
            logger.error("Gemini health check failed", error=str(e))
            return False
