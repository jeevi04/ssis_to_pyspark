from llm.ollama_provider import OllamaProvider

provider = OllamaProvider(model="llama3.2")

response = provider.generate(
    prompt="Explain RAG in one sentence",
    system_prompt="You are a concise assistant"
)

print("Text:", response.text)
print("Model:", response.model)
print("Provider:", response.provider)
print("Latency (ms):", response.latency_ms)
print("Tokens:", response.tokens_used)
