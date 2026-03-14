"""
Azure Anthropic Claude LLM Provider.

Connects to Azure AI Foundry-hosted Claude via the Anthropic Messages API.
Endpoint format: https://<resource>.services.ai.azure.com/anthropic/
"""
import time
from typing import Optional

from src.llm.base import BaseLLMProvider, LLMResponse
from src.logging import get_logger

logger = get_logger(__name__)


class AzureAnthropicProvider(BaseLLMProvider):
    """Azure-hosted Anthropic Claude provider (via Azure AI Foundry)."""

    def __init__(
        self,
        deployment_name: str,
        endpoint: str,
        api_key: str,
        api_version: str = "2024-06-01",
        temperature: float = 0.0,
        max_tokens: int = 8192,
        timeout: int = 300,
    ):
        super().__init__(deployment_name, temperature, max_tokens)
        self.deployment_name = deployment_name
        self.endpoint = endpoint.rstrip("/")
        self.api_version = api_version
        self.timeout = timeout

        # anthropic SDK >= 0.39 supports azure base_url + auth_token
        from anthropic import Anthropic

        # Azure AI Foundry exposes the Anthropic REST API at:
        #   <endpoint>/models/<deployment>/chat/completions  (OpenAI-compat)
        # OR the native Anthropic path via the SDK's azure mode.
        # We use the SDK's `base_url` + `api_key` headers to call the native API.
        self.client = Anthropic(
            base_url=f"{self.endpoint}/",
            api_key=api_key,
            timeout=timeout,
        )
        self._api_key = api_key

        logger.info(
            "Initialized Azure Anthropic provider",
            deployment=deployment_name,
            endpoint=endpoint,
        )

    @property
    def provider_name(self) -> str:
        return "azure_anthropic"

    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> LLMResponse:
        """Generate response using Azure-hosted Claude."""
        import httpx

        start_time = time.time()

        logger.debug(
            "Sending request to Azure Anthropic",
            deployment=self.deployment_name,
            prompt_length=len(prompt),
        )

        # Azure AI Foundry Anthropic endpoint format:
        #   POST {endpoint}/v1/messages
        # where endpoint = "https://<resource>.services.ai.azure.com/anthropic"
        url = f"{self.endpoint}/v1/messages"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": self.deployment_name,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system_prompt:
            payload["system"] = system_prompt

        try:
            with httpx.Client(timeout=self.timeout) as http:
                response = http.post(url, headers=headers, json=payload)

                if response.status_code != 200:
                    logger.error(
                        "Azure Anthropic API error",
                        status_code=response.status_code,
                        url=url,
                        body=response.text[:500],
                    )

                response.raise_for_status()
                data = response.json()

            latency_ms = (time.time() - start_time) * 1000

            # Standard Anthropic Messages API response shape
            text = data["content"][0]["text"]
            tokens_used = None
            if "usage" in data:
                tokens_used = data["usage"].get("input_tokens", 0) + data["usage"].get("output_tokens", 0)

            logger.info(
                "Azure Anthropic response received",
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

        except Exception as e:
            logger.error("Azure Anthropic request failed", error=str(e))
            raise

    def health_check(self) -> bool:
        """Check if the Azure Anthropic endpoint is reachable."""
        try:
            resp = self.generate("Say 'ok'", system_prompt=None)
            return bool(resp.text)
        except Exception as e:
            logger.error("Azure Anthropic health check failed", error=str(e))
            return False
