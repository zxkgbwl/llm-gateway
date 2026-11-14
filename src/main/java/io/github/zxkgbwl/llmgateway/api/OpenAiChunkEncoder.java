package io.github.zxkgbwl.llmgateway.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.zxkgbwl.llmgateway.provider.FinishReason;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent;
import io.github.zxkgbwl.llmgateway.provider.Usage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 把统一事件编码成 OpenAI 的 chat.completion.chunk。
 * 有状态（是否已经发过 role、最后一次 usage），所以每个流新建一个实例。
 */
public final class OpenAiChunkEncoder {
    private final ObjectMapper json;
    private final String model;
    private final boolean includeUsage;
    private final String id = "chatcmpl-" + UUID.randomUUID();
    private final long created = Instant.now().getEpochSecond();

    private boolean roleSent;
    private Usage lastUsage;
    public OpenAiChunkEncoder(ObjectMapper json, String model, boolean includeUsage) {
        this.json = json;
        this.model = model;
        this.includeUsage = includeUsage;

    }

    /** 一个事件可能对应 0 个或多个 SSE 的 data 内容。 */
    public List<String> encode(StreamEvent event) {
        List<String> out = new ArrayList<>();
        if (event instanceof StreamEvent.TextDelta delta) {
            ObjectNode d = json.createObjectNode();
            if (!roleSent) {
                d.put("role", "assistant"); // OpenAI 只在第一个 chunk 里带 role
                roleSent = true;
            }
            d.put("content", delta.text());
            out.add(chunk(d, null));
        } else if (event instanceof StreamEvent.UsageUpdate usageUpdate) {
            // 先暂存：OpenAI 把 usage 放在结束 chunk 之后单独发送，而我们的事件顺序是 usage 在 Finished 之前
            lastUsage = usageUpdate.usage();
        } else if (event instanceof StreamEvent.Finished finished) {
            out.add(chunk(json.createObjectNode(), mapFinishReason(finished.reason())));
            if (includeUsage && lastUsage != null) {
                out.add(usageChunk(lastUsage));
            }
            out.add("[DONE]");
        } else if (event instanceof StreamEvent.StreamError error) {
            ObjectNode root = json.createObjectNode();
            ObjectNode err = root.putObject("error");
            err.put("message", error.message());
            err.put("type", "upstream_error");
            out.add(root.toString());
        } else {
            throw new IllegalStateException("Unhandled StreamEvent: " + event.getClass());
        }
        return out;
    }



    private String usageChunk(Usage usage) {
        ObjectNode root = header();
        root.putArray("choices");// usage chunk 的 choices 是空数组
        ObjectNode u = root.putObject("usage");
        u.put("prompt_tokens", usage.inputTokens());
        u.put("completion_tokens", usage.outputTokens());
        u.put("total_tokens", usage.inputTokens() + usage.outputTokens());
        return root.toString();
    }

    private String chunk(ObjectNode delta, String finishReason) {
        ObjectNode root = header();
        ObjectNode choice = root.putArray("choices").addObject();
        choice.put("index", 0);
        choice.set("delta", delta);
        if (finishReason == null) {
            choice.putNull("finish_reason");
        }else {
            choice.put("finish_reason", finishReason);
        }
        return root.toString();
    }
    private ObjectNode header() {
        ObjectNode root = json.createObjectNode();
        root.put("id", id);
        root.put("object", "chat.completion.chunk");
        root.put("created", created);
        root.put("model", model);
        return root;
    }

    // 统一枚举 → OpenAI 的 finish_reason；枚举常量名按你的 FinishReason 调整
    private static String mapFinishReason(FinishReason reason) {
        return switch (reason) {
            case LENGTH -> "length";
            case CONTENT_FILTER -> "content_filter";
            default -> "stop";
        };
    }



}
