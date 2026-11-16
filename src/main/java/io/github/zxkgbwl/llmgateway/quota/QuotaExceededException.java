package io.github.zxkgbwl.llmgateway.quota;

public class QuotaExceededException extends RuntimeException{

    private final long retryAfterSeconds;
    private final long remainingTokens;

    public QuotaExceededException(long retryAfterSeconds, long remainingTokens) {
        super("Tenant TPM limit exceeded, retry after " + retryAfterSeconds + "s");
        this.retryAfterSeconds = retryAfterSeconds;
        this.remainingTokens = remainingTokens;
    }

    public long retryAfterSeconds () {
        return retryAfterSeconds;
    }

    public long remainingTokens () {
        return remainingTokens;
    }


}
