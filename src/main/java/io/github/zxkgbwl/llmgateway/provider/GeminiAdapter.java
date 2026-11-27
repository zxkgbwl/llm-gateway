package io.github.zxkgbwl.llmgateway.provider;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.common.ChatRequest.Role;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.Finished;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.StreamError;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.TextDelta;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.UsageUpdate;
import io.github.zxkgbwl.llmgateway.sse.SseEvent;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Google Gemini native protocol (streamGenerateContent).
 *
 * <p>Wire format:
 * <ul>
 *   <li>the model is in the URL path; "?alt=sse" is required to get SSE framing</li>
 *   <li>"data:" lines only, no [DONE]; the chunk carrying finishReason is the last one</li>
 *   <li>text is in candidates[0].content.parts[]; some parts carry no text</li>
 *   <li>usageMetadata may appear on every chunk and is already cumulative</li>
 *   <li>roles are "user" and "model"; the system prompt goes in systemInstruction</li>
 * </ul>
 * Thinking tokens (thoughtsTokenCount) are billed as output, so they are added to outputTokens.
 * Verify field names against the current official docs.
 */
public final class GeminiAdapter implements ProtocolAdapter {

    private final String baseUrl;
    private final String apiKey;

    /** @param baseUrl e.g. https://generativelanguage.googleapis.com */
    public GeminiAdapter(String baseUrl, String apiKey) {
        this.baseUrl = UpstreamJson.stripTrailingSlash(baseUrl);
        this.apiKey = apiKey;
    }

    @Override
    public String providerName() {
        return "gemini";
    }

    @Override
    public UpstreamRequest buildRequest(ChatRequest request) {
        String system = request.messages().stream()
                .filter(m -> m.role() == Role.SYSTEM)
                .map(ChatRequest.Message::content)
                .collect(Collectors.joining("\n\n"));

        List<Map<String, Object>> contents = request.messages().stream()
                .filter(m -> m.role() != Role.SYSTEM)
                .map(m -> Map.<String, Object>of(
                        "role", m.role() == Role.ASSISTANT ? "model" : "user",
                        "parts", List.of(Map.of("text", m.content()))))
                .toList();

        Map<String, Object> body = new LinkedHashMap<>();
        if (!system.isEmpty()) {
            body.put("systemInstruction", Map.of("parts", List.of(Map.of("text", system))));
        }
        body.put("contents", contents);
        body.put("generationConfig", Map.of("maxOutputTokens", request.maxTokens()));

        String url = baseUrl + "/v1beta/models/"
                + URLEncoder.encode(request.model(), StandardCharsets.UTF_8)
                + ":streamGenerateContent?alt=sse";

        return new UpstreamRequest(
                URI.create(url),
                Map.of("x-goog-api-key", apiKey),
                UpstreamJson.toJson(body));
    }

    @Override
    public EventTranslator newTranslator() {
        return new Translator();
    }

    /**
     * Gemini 的 Key 无效返回的是 400 INVALID_ARGUMENT（不是 401），只能靠 error.details[].reason 识别。
     * 不覆盖的话会被当成 CLIENT_ERROR，不切换厂商，降级链路就断了。
     */
    @Override
    public ErrorClass classify(UpstreamHttpException e) {
        if (e.statusCode() == 400 && hasReason(e.body(), "API_KEY_INVALID")) {
            return ErrorClass.PROVIDER_MISCONFIGURED;
        }
        return ProtocolAdapter.super.classify(e);
    }

    private static boolean hasReason(String body, String reason) {
        try {
            for (JsonNode detail : UpstreamJson.parse(body).path("error").path("details")) {
                if (reason.equals(detail.path("reason").asText())) {
                    return true;
                }
            }
        } catch (RuntimeException ignored) {
            // 错误体不是 JSON，按默认规则处理
        }
        return false;
    }

    static final class Translator implements EventTranslator {

        @Override
        public List<StreamEvent> translate(SseEvent sse) {
            JsonNode chunk = UpstreamJson.parse(sse.data());
            if (chunk.has("error")) {
                return List.of(new StreamError(chunk.get("error").toString()));
            }

            List<StreamEvent> events = new ArrayList<>();
            JsonNode candidate = chunk.path("candidates").path(0);

            for (JsonNode part : candidate.path("content").path("parts")) {
                boolean isThought = part.path("thought").asBoolean(false);
                String text = part.path("text").asText("");
                if (!isThought && !text.isEmpty()) {
                    events.add(new TextDelta(text));
                }
            }

            JsonNode usage = chunk.path("usageMetadata");
            if (usage.isObject()) {
                int output = usage.path("candidatesTokenCount").asInt() + usage.path("thoughtsTokenCount").asInt();
                events.add(new UsageUpdate(new Usage(usage.path("promptTokenCount").asInt(), output,
                        usage.path("cachedContentTokenCount").asInt())));
            }

            String blockReason = chunk.path("promptFeedback").path("blockReason").asText("");
            String finishReason = candidate.path("finishReason").asText("");
            if (!blockReason.isEmpty()) {
                // 整个 prompt 被安全过滤拒绝，没有 candidates
                events.add(new Finished(FinishReason.CONTENT_FILTER, "PROMPT_BLOCKED:" + blockReason));
            } else if (!finishReason.isEmpty()) {
                events.add(new Finished(mapFinishReason(finishReason), finishReason));
            }
            return events;
        }

        private static FinishReason mapFinishReason(String raw) {
            return switch (raw) {
                case "STOP" -> FinishReason.STOP;
                case "MAX_TOKENS" -> FinishReason.LENGTH;
                case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII" -> FinishReason.CONTENT_FILTER;
                default -> FinishReason.OTHER;
            };
        }
    }
}
