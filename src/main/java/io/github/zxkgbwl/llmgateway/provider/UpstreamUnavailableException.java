package io.github.zxkgbwl.llmgateway.provider;

public class UpstreamUnavailableException extends RuntimeException {

    private final boolean rateLimited;
    private final long retryAfterSeconds;

    public UpstreamUnavailableException(String message, boolean rateLimited, long retryAfterSeconds) {
        super((message));
        this.rateLimited = rateLimited;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public boolean rateLimited() {
        return rateLimited;
    }

    public long retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
