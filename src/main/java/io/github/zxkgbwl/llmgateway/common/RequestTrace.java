package io.github.zxkgbwl.llmgateway.common;

import io.github.zxkgbwl.llmgateway.provider.Candidate;

import java.util.function.LongSupplier;

/**
 * 请求级追踪：请求开始时间、实际服务的候选、那次尝试的开始时间。
 *
 * <p>每次订阅新建（有状态）。FailoverClient 通过 AttemptListener 回调 onAttempt；
 * 尝试是串行的，最后一次回调就是实际服务的候选。指标（StreamMetrics）和结算（QuotaSettler）都从这里读，彼此不依赖。
 */
public final class RequestTrace {
    private final LongSupplier nanoClock;
    private final long startNanos;
    private volatile Candidate servedBy;      // null：还没打到上游（如 TPM 拒绝）
    private volatile long attemptStartNanos;

    public RequestTrace(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
        this.startNanos = nanoClock.getAsLong();
    }

    public static RequestTrace start() {
        return new RequestTrace(System::nanoTime);
    }

    public void onAttempt(Candidate candidate) {
        servedBy = candidate;
        attemptStartNanos = nanoClock.getAsLong();
    }

    public long now() { return nanoClock.getAsLong(); }
    public long startNanos() { return startNanos; }
    public long attemptStartNanos() { return attemptStartNanos; }
    public Candidate servedBy() { return servedBy; }

    public String providerKey() {
        Candidate c = servedBy;
        return c == null ? "none" : c.key();
    }
}