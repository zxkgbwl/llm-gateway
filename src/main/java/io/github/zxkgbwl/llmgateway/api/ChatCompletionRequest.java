package io.github.zxkgbwl.llmgateway.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zxkgbwl.llmgateway.common.ChatRequest.Message;

import java.util.List;

public record ChatCompletionRequest(
    String model,
    List<Message> messages,
    @JsonProperty("max_tokens") Integer maxTokens,
    Boolean stream,
    @JsonProperty("stream_options") StreamOptions streamOptions) {


    // OpenAI 格式的消息：role 是小写字符串（system / user / assistant）
    public record Message(String role, String content) {}

    public record StreamOptions(@JsonProperty("include_usage") Boolean includeUsage) {}

    public boolean includeUsage() {
        return streamOptions != null && Boolean.TRUE.equals(streamOptions.includeUsage());
    }

}
