package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;

/**
 * Everything that differs between providers, and nothing else.
 *
 * <p>To add a provider: implement this interface (request building plus an
 * {@link EventTranslator}) and add a translator test with a recorded stream.
 * Transport, SSE parsing, error handling and truncation detection are shared
 * in {@link StreamingClient}.
 */
public interface ProtocolAdapter {

    /** Short name used in logs and metrics, e.g. "anthropic". */
    String providerName();

    UpstreamRequest buildRequest(ChatRequest request);

    /** Creates a fresh translator for one stream. Never share a translator between streams. */
    EventTranslator newTranslator();
}
