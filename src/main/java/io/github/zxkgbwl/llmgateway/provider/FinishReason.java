package io.github.zxkgbwl.llmgateway.provider;

/**
 * Why generation stopped, normalized across providers. The provider's original value is kept
 * in {@link StreamEvent.Finished#rawReason()} for logging.
 */
public enum FinishReason {
    /** Natural end of the answer, or a stop sequence was hit. */
    STOP,
    /** Cut off by maxTokens. */
    LENGTH,
    /** Blocked or truncated by a safety / content filter. */
    CONTENT_FILTER,
    /** The model wants the caller to run a tool. */
    TOOL_CALL,
    /** Anything the adapter does not recognize. */
    OTHER
}
