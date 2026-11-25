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

    /**
     * 首字节前上游返回非 2xx 时的分类。默认只看状态码；
     * 厂商的状态码不符合惯例时（比如 Gemini 的 Key 无效返回 400），在各自的 adapter 里覆盖。
     */
    default ErrorClass classify(UpstreamHttpException e) {
        return ErrorClass.ofStatus(e.statusCode());
    }
}
