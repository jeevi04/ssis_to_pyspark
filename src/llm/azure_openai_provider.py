"""
Azure OpenAI LLM Provider.

Connects to Azure OpenAI Service for LLM inference.
"""
import time
from typing import Optional

from openai import AzureOpenAI
from openai import APIError, APIConnectionError, RateLimitError, APITimeoutError

from src.llm.base import BaseLLMProvider, LLMResponse
from src.logging import get_logger

logger = get_logger(__name__)


class AzureOpenAIProvider(BaseLLMProvider):
    """Azure OpenAI provider."""
    
    def __init__(
        self,
        deployment_name: str,
        endpoint: str,
        api_key: str,
        api_version: str = "2024-02-15-preview",
        temperature: float = 0.0,
        max_tokens: int = 8192,
        use_responses_api: bool = False,
        timeout: int = 60,
    ):
        super().__init__(deployment_name, temperature, max_tokens)
        self.deployment_name = deployment_name
        self.endpoint = endpoint
        self.api_version = api_version
        self.use_responses_api = use_responses_api
        self.timeout = timeout
        
        self.client = AzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            azure_endpoint=endpoint,
            timeout=timeout,
        )
        
        logger.info(
            "Initialized Azure OpenAI provider",
            deployment=deployment_name,
            endpoint=endpoint,
            api_version=api_version,
            use_responses_api=use_responses_api,
        )
    
    @property
    def provider_name(self) -> str:
        return "azure_openai"
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> LLMResponse:
        """Generate response using Azure OpenAI."""
        start_time = time.time()
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        if self.use_responses_api:
            return self._generate_via_responses(prompt, system_prompt, start_time)
        
        logger.debug(
            "Sending request to Azure OpenAI",
            deployment=self.deployment_name,
            use_responses_api=self.use_responses_api,
            prompt_length=len(prompt),
        )
        
        try:
            # Use Chat Completions API
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )
            text = response.choices[0].message.content
            
            latency_ms = (time.time() - start_time) * 1000
            
            # Extract token usage
            tokens_used = None
            if response.usage:
                tokens_used = response.usage.total_tokens
            
            logger.info(
                "Azure OpenAI response received",
                deployment=self.deployment_name,
                latency_ms=round(latency_ms, 2),
                tokens=tokens_used,
            )
            
            return LLMResponse(
                text=text,
                model=self.deployment_name,
                provider=self.provider_name,
                tokens_used=tokens_used,
                latency_ms=latency_ms,
            )
            
        except APITimeoutError as e:
            logger.error("Azure OpenAI request timed out", error=str(e))
            raise
        except RateLimitError as e:
            logger.error("Azure OpenAI rate limit exceeded", error=str(e))
            raise
        except APIConnectionError as e:
            logger.error("Azure OpenAI connection error", error=str(e))
            raise
        except APIError as e:
            logger.error("Azure OpenAI API error", error=str(e))
            raise
        except Exception as e:
            logger.error("Azure OpenAI request failed", error=str(e))
            raise
    
    def health_check(self) -> bool:
        """Check if Azure OpenAI is accessible."""
        try:
            logger.debug("Azure OpenAI health check starting", deployment=self.deployment_name, use_responses_api=self.use_responses_api)
            
            if self.use_responses_api:
                self._generate_via_responses("test", timeout=10)
                return True

            # Make a minimal request to test connectivity
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[{"role": "user", "content": "test"}],
                max_tokens=5,
            )
            
            logger.debug("Azure OpenAI health check passed", deployment=self.deployment_name)
            return True
            
        except Exception as e:
            logger.error("Azure OpenAI health check failed", error=str(e))
            return False

    def _generate_via_responses(
        self, 
        prompt: str, 
        system_prompt: Optional[str] = None, 
        start_time: float = None,
        timeout: Optional[int] = None
    ) -> LLMResponse:
        """Generate response using the new Azure OpenAI Responses API."""
        import httpx
        
        start_time = start_time or time.time()
        timeout = timeout or self.timeout
        
        url = self.endpoint
        if not url.endswith("/"):
            url += "/"
            
        # The Responses API for gpt-5.3-codex typically uses /openai/responses 
        # or /openai/deployments/{deployment}/responses.
        # Based on successful tests, we use /openai/responses directly.
        url = f"{url}openai/responses?api-version={self.api_version}"
            
        headers = {
            "api-key": self.client.api_key,
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.deployment_name,
            "instructions": system_prompt or "You are a helpful assistant.",
            "input": prompt
        }
        
        logger.debug(
            "Sending request to Azure OpenAI Responses API",
            url=url,
            deployment=self.deployment_name,
        )
        
        try:
            with httpx.Client() as client:
                response = client.post(url, headers=headers, json=payload, timeout=timeout)
                
                if response.status_code != 200:
                    logger.error(
                        "Azure OpenAI Responses API error",
                        status_code=response.status_code,
                        response=response.text
                    )
                
                response.raise_for_status()
                
                try:
                    data = response.json()
                except Exception as e:
                    logger.error("Failed to parse JSON from Responses API", response=response.text)
                    raise
                
                # Parse output text from Responses API format
                text = None
                # New Responses API format: data["output"][0]["content"][0]["text"]
                if "output" in data and isinstance(data["output"], list) and len(data["output"]) > 0:
                    output_item = data["output"][0]
                    if "content" in output_item and isinstance(output_item["content"], list) and len(output_item["content"]) > 0:
                        content_item = output_item["content"][0]
                        text = content_item.get("text")
                            
                if text is None:
                    # Fallback for different preview versions
                    if "output_items" in data and data["output_items"]:
                        for item in data["output_items"]:
                            if item.get("type") == "text":
                                text = item.get("text")
                                break
                    
                if text is None:
                    text = data.get("output", {}).get("text") or data.get("text")
                    
                if text is None:
                    logger.warning("Could not extract text from Responses API response", response=data)
                    text = str(data)

                latency_ms = (time.time() - start_time) * 1000
                
                # Extract token usage if available
                tokens_used = data.get("usage", {}).get("total_tokens")
                
                return LLMResponse(
                    text=text,
                    model=self.deployment_name,
                    provider=self.provider_name,
                    tokens_used=tokens_used,
                    latency_ms=latency_ms,
                )
        except Exception as e:
            logger.error("Azure OpenAI Responses API request failed", error=str(e))
            raise
