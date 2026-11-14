package io.github.zxkgbwl.llmgateway.sse;

/**
 * One complete Server-Sent Event.
 *
 * @param event the event name, or {@code null} when the server sent no "event:" line
 *              (OpenAI and Gemini never send one; Anthropic always does)
 * @param data  the event payload; multiple "data:" lines are joined with '\n'
 */
public record SseEvent(String event, String data) {
}
