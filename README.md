# llm-gateway

An LLM gateway in Java: one OpenAI-compatible endpoint in front of multiple model providers,
with streaming passthrough, token-based rate limiting, fallback and circuit breaking,
cost attribution and semantic caching.

> Status: week 1 of 8 — understanding each provider's streaming protocol with raw HTTP.

## Architecture (provider layer)

```
playground / gateway            consumes StreamEvent only
        │
StreamingClient ─► EventStream  shared transport: HTTP, status check, truncation detection
        │               │
ProtocolAdapter   EventTranslator   per provider: build request / translate SSE events
        │
SseReader                        SSE framing, knows nothing about LLMs
```

Every provider is translated into four provider-neutral events:

| Event | Meaning |
|---|---|
| `TextDelta` | an incremental piece of the answer |
| `UsageUpdate` | a cumulative token-usage snapshot (latest wins) |
| `Finished` | the provider confirmed the end; carries a normalized `FinishReason` |
| `StreamError` | the stream failed after it started (in-stream error, lost connection, truncation) |

Failures **before** the stream starts are thrown as `UpstreamHttpException` instead, because
those are still safe to retry or route to a fallback model.

### Adding a provider

1. Implement `ProtocolAdapter`: `buildRequest` and a per-stream `EventTranslator`.
2. Capture a real stream with curl and add a translator test (see `src/test/.../provider`).
3. Register it in `StreamDemo.configuredTargets()`.

Transport, SSE parsing, error handling and truncation detection come for free.

| Adapter | Covers |
|---|---|
| `AnthropicAdapter` | Anthropic Messages API |
| `GeminiAdapter` | Gemini `streamGenerateContent` |
| `OpenAiAdapter` | OpenAI and any OpenAI-compatible endpoint (Ollama, vLLM) |

## Running

```powershell
$env:ANTHROPIC_API_KEY = "sk-ant-..."
$env:GEMINI_API_KEY    = "..."
# Optional: $env:ANTHROPIC_MODEL / $env:GEMINI_MODEL — default model names go stale, check the docs

mvn -q test
mvn -q compile exec:java
mvn -q compile exec:java "-Dexec.args=--raw --providers=anthropic,gemini --max-tokens=100"
```

`--raw` prints every raw SSE line, to inspect the wire format.

### Raw streams with curl

Use `curl.exe` (not the PowerShell `curl` alias) and `-N` to disable buffering.

```powershell
curl.exe -N https://api.anthropic.com/v1/messages `
  -H "x-api-key: $env:ANTHROPIC_API_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" `
  --data "@requests/anthropic-stream.json"

curl.exe -N "https://generativelanguage.googleapis.com/v1beta/models/<model>:streamGenerateContent?alt=sse" `
  -H "x-goog-api-key: $env:GEMINI_API_KEY" -H "Content-Type: application/json" `
  --data "@requests/gemini-stream.json"
```
