package io.github.zxkgbwl.llmgateway.provider;

/**
 * The gateway's provider-neutral stream event. Every adapter translates its wire format into
 * these four types, and nothing above the adapter layer ever sees provider-specific JSON.
 *
 * <p>Contract for a well-formed stream: zero or more {@link TextDelta} and {@link UsageUpdate}
 * in any order, ending with exactly one terminal event ({@link Finished} or {@link StreamError}).
 */
public sealed interface StreamEvent {

    /** An incremental piece of the answer. */
    record TextDelta(String text) implements StreamEvent {
    }

    /**
     * A usage snapshot. Always cumulative: the latest one replaces earlier ones, never add them up.
     * Adapters are responsible for merging providers that report usage in pieces.
     */
    record UsageUpdate(Usage usage) implements StreamEvent {
    }

    /** The provider confirmed the stream ended. */
    record Finished(FinishReason reason, String rawReason) implements StreamEvent {
    }

    /**
     * The stream failed after it had started: an in-stream error from the provider, a lost
     * connection, or a stream that ended without a finish signal. Failures before the stream
     * starts are thrown as {@link UpstreamHttpException} instead.
     */
    record StreamError(String message) implements StreamEvent {
    }

    default boolean isTerminal() {
        return this instanceof Finished || this instanceof StreamError;
    }
}
