package io.github.zxkgbwl.llmgateway.provider;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.Finished;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.StreamError;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.TextDelta;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.UsageUpdate;
import io.github.zxkgbwl.llmgateway.sse.SseEvent;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * OpenAI Chat Completions protocol. Also covers any OpenAI-compatible endpoint
 * (Ollama, vLLM, Gemini's compatibility layer) by changing baseUrl.
 *
 * <p>Wire format:
 * <ul>
 *   <li>"data:" lines only, no "event:" lines; the stream ends with "data: [DONE]"</li>
 *   <li>text is in choices[0].delta.content</li>
 *   <li>usage is sent only when stream_options.include_usage is true, in an extra chunk
 *       with empty choices that arrives AFTER the finish_reason chunk and before [DONE]</li>
 * </ul>
 */
public final class OpenAiAdapter implements ProtocolAdapter {

    private final String providerName;
    private final String baseUrl;
    private final String apiKey;

    /** @param baseUrl e.g. https://api.openai.com/v1 or http://localhost:11434/v1 */
    public OpenAiAdapter(String providerName, String baseUrl, String apiKey) {
        this.providerName = providerName;
        this.baseUrl = UpstreamJson.stripTrailingSlash(baseUrl);
        this.apiKey = apiKey;
    }

    @Override
    public String providerName() {
        return providerName;
    }

    @Override
    public UpstreamRequest buildRequest(ChatRequest request) {
        List<Map<String, String>> messages = request.messages().stream()
                .map(m -> Map.of("role", m.role().name().toLowerCase(Locale.ROOT), "content", m.content()))
                .toList();

        Map<String, Object> body = Map.of(
                "model", request.model(),
                "messages", messages,
                "max_tokens", request.maxTokens(),
                "stream", true,
                "stream_options", Map.of("include_usage", true));

        return new UpstreamRequest(
                URI.create(baseUrl + "/chat/completions"),
                Map.of("Authorization", "Bearer " + apiKey),
                UpstreamJson.toJson(body));
    }

    @Override
    public EventTranslator newTranslator() {
        return new Translator();
    }

    static final class Translator implements EventTranslator {

        // finish_reason arrives before the usage chunk, so hold it until [DONE].
        private String rawFinishReason;

        @Override
        public List<StreamEvent> translate(SseEvent sse) {
            if ("[DONE]".equals(sse.data())) {
                return List.of(new Finished(mapFinishReason(rawFinishReason), rawFinishReason));
            }

            JsonNode chunk = UpstreamJson.parse(sse.data());
            if (chunk.has("error")) {
                return List.of(new StreamError(chunk.get("error").toString()));
            }

            List<StreamEvent> events = new ArrayList<>();
            JsonNode choice = chunk.path("choices").path(0);

            String text = choice.path("delta").path("content").asText("");
            if (!text.isEmpty()) {
                events.add(new TextDelta(text));
            }

            String finishReason = choice.path("finish_reason").asText("");
            if (!finishReason.isEmpty()) {
                rawFinishReason = finishReason;
            }

            JsonNode usage = chunk.path("usage");
            if (usage.isObject()) {
                events.add(new UsageUpdate(new Usage(
                        usage.path("prompt_tokens").asInt(),
                        usage.path("completion_tokens").asInt(),
                        usage.path("prompt_tokens_details").path("cached_tokens").asInt())));
            }
            return events;
        }

        private static FinishReason mapFinishReason(String raw) {
            if (raw == null) {
                return FinishReason.OTHER;
            }
            return switch (raw) {
                case "stop" -> FinishReason.STOP;
                case "length" -> FinishReason.LENGTH;
                case "content_filter" -> FinishReason.CONTENT_FILTER;
                case "tool_calls", "function_call" -> FinishReason.TOOL_CALL;
                default -> FinishReason.OTHER;
            };
        }
    }
}
