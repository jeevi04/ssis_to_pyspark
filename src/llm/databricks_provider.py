"""
Databricks Model Serving LLM Provider.

Connects to Databricks Model Serving endpoints (OpenAI-compatible) for LLM inference.
Typically used for Claude or other models hosted on Databricks.

NOTE: Databricks Model Serving has a server-side ~180s hard timeout on non-streaming
requests. This provider uses streaming mode by default to bypass that limit — the
connection stays alive as tokens arrive, so only inter-token idle time matters.
"""
import time
from typing import Optional

from openai import OpenAI
from openai import APIError, APIConnectionError, RateLimitError, APITimeoutError

from src.llm.base import BaseLLMProvider, LLMResponse
from src.logging import get_logger

logger = get_logger(__name__)


class DatabricksProvider(BaseLLMProvider):
    """Databricks Model Serving provider (OpenAI-compatible, with streaming)."""

    def __init__(
        self,
        model: str,
        workspace_url: str,
        api_token: str,
        temperature: float = 0.0,
        max_tokens: int = 8192,
        timeout: int = 600,
    ):
        super().__init__(model, temperature, max_tokens)
        self.workspace_url = workspace_url.rstrip("/")
        # Databricks Model Serving uses /serving-endpoints as base path
        base_url = f"{self.workspace_url}/serving-endpoints"
        self.timeout = timeout

        self.client = OpenAI(
            api_key=api_token,
            base_url=base_url,
            timeout=timeout,
        )

        logger.info(
            "Initialized Databricks provider",
            model=model,
            workspace_url=workspace_url,
        )

    @property
    def provider_name(self) -> str:
        return "databricks"

    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> LLMResponse:
        """Generate response using Databricks Model Serving (streaming mode).

        Uses streaming to bypass Databricks' ~180-second server-side hard timeout.
        Non-streaming requests are killed after ~180s regardless of client timeout.
        With streaming the connection stays alive as long as tokens keep arriving.
        """
        start_time = time.time()

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        logger.debug(
            "Sending streaming request to Databricks",
            model=self.model,
            prompt_length=len(prompt),
        )

        try:
            # stream=True keeps the HTTP connection alive as tokens flow in,
            # bypassing the ~180s server-side idle timeout on Databricks endpoints.
            stream = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                stream=True,
            )

            # Accumulate streamed chunks into a single response string
            full_text = ""
            tokens_used = None
            for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    full_text += chunk.choices[0].delta.content
                # Some providers send usage in final chunk
                if hasattr(chunk, "usage") and chunk.usage:
                    tokens_used = chunk.usage.total_tokens

            latency_ms = (time.time() - start_time) * 1000

            logger.info(
                "Databricks streaming response complete",
                model=self.model,
                latency_ms=round(latency_ms, 2),
                tokens=tokens_used,
                response_length=len(full_text),
            )

            return LLMResponse(
                text=full_text,
                model=self.model,
                provider=self.provider_name,
                tokens_used=tokens_used,
                latency_ms=latency_ms,
            )

        except APITimeoutError as e:
            logger.error("Databricks request timed out", error=str(e))
            raise
        except RateLimitError as e:
            logger.error("Databricks rate limit exceeded", error=str(e))
            raise
        except APIConnectionError as e:
            logger.error("Databricks connection error", error=str(e))
            raise
        except APIError as e:
            logger.error("Databricks API error", error=str(e))
            raise
        except Exception as e:
            logger.error("Databricks request failed", error=str(e))
            raise

    def health_check(self) -> bool:
        """Check if Databricks serving endpoint is accessible."""
        try:
            # Health check uses non-streaming (tiny response, well within 180s)
            self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": "test"}],
                max_tokens=5,
            )
            logger.debug("Databricks health check passed", model=self.model)
            return True

        except Exception as e:
            logger.error("Databricks health check failed", error=str(e))
            return False
