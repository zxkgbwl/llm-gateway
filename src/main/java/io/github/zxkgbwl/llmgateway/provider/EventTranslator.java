package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.sse.SseEvent;

import java.util.List;

/**
 * Translates one provider's SSE events into {@link StreamEvent}s.
 *
 * <p>A translator belongs to exactly one stream, so it may keep per-stream state
 * (e.g. token counts that arrive in pieces) in plain fields without any locking.
 * It does no I/O, which makes it testable with recorded SSE text.
 */
public interface EventTranslator {

    /**
     * @return zero or more events; return an empty list for events that carry nothing useful
     *         (pings, block boundaries). Runtime exceptions are turned into a StreamError.
     */
    List<StreamEvent> translate(SseEvent event);
}
