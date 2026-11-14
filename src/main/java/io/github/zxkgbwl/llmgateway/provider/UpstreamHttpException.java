package io.github.zxkgbwl.llmgateway.provider;

import java.io.IOException;
import java.util.Optional;

/**
 * The provider rejected the request before any stream started (non-2xx status).
 *
 * <p>Because nothing has been sent to the client yet, these failures are safe to retry
 * or to route to a fallback model.
 */
public final class UpstreamHttpException extends IOException {

    private final String provider;
    private final int statusCode;
    private final String retryAfter;
    private final String body;

    public UpstreamHttpException(String provider, int statusCode, String retryAfter, String body) {
        super(provider + " returned HTTP " + statusCode + ": " + body);
        this.provider = provider;
        this.statusCode = statusCode;
        this.retryAfter = retryAfter;
        this.body = body;
    }

    public String provider() {
        return provider;
    }

    public int statusCode() {
        return statusCode;
    }

    /**
     * Raw value of the Retry-After header, if the provider sent one.
     */
    public String retryAfter() {
        return Optional.ofNullable(retryAfter).toString();
    }

    public String body() {
        return body;
    }

    /** 429 means "slow down", not "broken": it should not count as a circuit-breaker failure. */
    public boolean isRateLimited() {
        return statusCode == 429;
    }
}
