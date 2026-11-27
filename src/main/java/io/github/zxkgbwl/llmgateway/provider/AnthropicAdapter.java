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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Anthropic Messages protocol.
 *
 * <p>Wire format:
 * <ul>
 *   <li>every event has an "event:" line; dispatch on it</li>
 *   <li>message_start → content_block_start → content_block_delta* → content_block_stop
 *       → message_delta → message_stop, with optional ping events in between</li>
 *   <li>usage comes in two pieces: input_tokens in message_start, output_tokens
 *       (cumulative) in message_delta</li>
 *   <li>the system prompt is a top-level field, not a message; max_tokens is required</li>
 * </ul>
 */
public final class AnthropicAdapter implements ProtocolAdapter {

    private static final String API_VERSION = "2023-06-01";

    private final String baseUrl;
    private final String apiKey;

    /** @param baseUrl e.g. https://api.anthropic.com */
    public AnthropicAdapter(String baseUrl, String apiKey) {
        this.baseUrl = UpstreamJson.stripTrailingSlash(baseUrl);
        this.apiKey = apiKey;
    }

    @Override
    public String providerName() {
        return "anthropic";
    }

    @Override
    public UpstreamRequest buildRequest(ChatRequest request) {
        String system = request.messages().stream()
                .filter(m -> m.role() == Role.SYSTEM)
                .map(ChatRequest.Message::content)
                .collect(Collectors.joining("\n\n"));

        List<Map<String, String>> messages = request.messages().stream()
                .filter(m -> m.role() != Role.SYSTEM)
                .map(m -> Map.of("role", m.role().name().toLowerCase(Locale.ROOT), "content", m.content()))
                .toList();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", request.model());
        if (!system.isEmpty()) {
            body.put("system", system);
        }
        body.put("messages", messages);
        body.put("max_tokens", request.maxTokens());
        body.put("stream", true);

        return new UpstreamRequest(
                URI.create(baseUrl + "/v1/messages"),
                Map.of("x-api-key", apiKey, "anthropic-version", API_VERSION),
                UpstreamJson.toJson(body));
    }

    @Override
    public EventTranslator newTranslator() {
        return new Translator();
    }

    static final class Translator implements EventTranslator {

        private int inputTokens;
        private int outputTokens;
        private int cacheReadTokens;
        private int cacheWriteTokens;
        private String rawStopReason;

        @Override
        public List<StreamEvent> translate(SseEvent sse) {
            JsonNode node = UpstreamJson.parse(sse.data());
            String type =  node.path("type").asText();

            return switch (type) {
                case "message_start" -> {
                    JsonNode usage = node.path("message").path("usage");
                    inputTokens = usage.path("input_tokens").asInt();
                    outputTokens = usage.path("output_tokens").asInt();
                    cacheReadTokens = usage.path("cache_read_input_tokens").asInt();
                    cacheWriteTokens = usage.path("cache_creation_input_tokens").asInt();
                    yield List.of(usageSnapshot());
                }
                case "content_block_delta" -> {
                    JsonNode delta = node.path("delta");
                    boolean isText = "text_delta".equals(delta.path("type").asText());
                    yield isText ? List.of(new TextDelta(delta.path("text").asText())) : List.of();
                }
                case "message_delta" -> {
                    rawStopReason = node.path("delta").path("stop_reason").asText(null);
                    JsonNode usage = node.path("usage");
                    if (usage.has("output_tokens")) {
                        outputTokens = usage.path("output_tokens").asInt();
                    }
                    if (usage.has("input_tokens")) {
                        inputTokens = usage.path("input_tokens").asInt();
                    }
                    yield List.of(usageSnapshot());
                }
                case "message_stop" -> List.of(new Finished(mapStopReason(rawStopReason), rawStopReason));
                case "error" -> {
                    JsonNode error = node.path("error");
                    yield List.of(new StreamError(error.path("type").asText() + ": " + error.path("message").asText()));
                }
                default -> List.of(); // ping, content_block_start, content_block_stop
            };
        }

        private UsageUpdate usageSnapshot() {
            // Anthropic 的 input_tokens 不含缓存部分，加回去，和其他厂商语义一致
            return new UsageUpdate(new Usage(inputTokens + cacheReadTokens + cacheWriteTokens, outputTokens, cacheReadTokens));
        }

        private static FinishReason mapStopReason(String raw) {
            if (raw == null) {
                return FinishReason.OTHER;
            }
            return switch (raw) {
                case "end_turn", "stop_sequence" -> FinishReason.STOP;
                case "max_tokens" -> FinishReason.LENGTH;
                case "tool_use" -> FinishReason.TOOL_CALL;
                case "refusal" -> FinishReason.CONTENT_FILTER;
                default -> FinishReason.OTHER;
            };
        }
    }
}
