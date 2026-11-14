package io.github.zxkgbwl.llmgateway.common;

import io.github.zxkgbwl.llmgateway.provider.ProtocolAdapter;


import java.util.List;

/**
 * The gateway's provider-neutral request. Each {@link ProtocolAdapter} translates it
 * into its own wire format.
 */
public record ChatRequest(String model, List<Message> messages, int maxTokens) {

    public enum Role { SYSTEM, USER, ASSISTANT }

    public record Message(Role role, String content) {}

    public ChatRequest {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages must not be empty");
        }
        if (maxTokens <= 0) {
            throw new IllegalArgumentException("maxTokens must be positive");
        }
        messages = List.copyOf(messages);
    }

    /** Convenience factory for a single user prompt. */
    public static ChatRequest ofPrompt(String model, String prompt, int maxTokens) {
        return new ChatRequest(model, List.of(new Message(Role.USER, prompt)), maxTokens);
    }
}
