package io.github.zxkgbwl.llmgateway.tenant;

import io.github.zxkgbwl.llmgateway.provider.FinishReason;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent;
import io.github.zxkgbwl.llmgateway.provider.Usage;
import io.github.zxkgbwl.llmgateway.quota.QuotaExceededException;
import io.github.zxkgbwl.llmgateway.quota.QuotaSettler;
import io.github.zxkgbwl.llmgateway.quota.TpmLimiter;
import io.github.zxkgbwl.llmgateway.token.TokenEstimator;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class QuotaSettlerTest {
    private static final Tenant TENANT = new Tenant("t1", "unused", 10_000, 1024);
    private static final int INPUT = 20;
    private static final int MAX_TOKENS = 100;
    private static final int RESERVED = INPUT + MAX_TOKENS;

    /** 记录调用的假限流器 */
    static final class FakeLimiter implements TpmLimiter {
        final List<Integer> adjusts = new CopyOnWriteArrayList<>();
        boolean reject;

        @Override
        public Mono<Void> reserve(Tenant tenant, int tokens) {
            return reject ? Mono.error(new QuotaExceededException(3, 0)) : Mono.empty();
        }

        @Override
        public Mono<Void> adjust(Tenant tenant, int delta) {
            return Mono.fromRunnable(() -> adjusts.add(delta));
        }
    }

    private final FakeLimiter limiter = new FakeLimiter();
    private final QuotaSettler settler = new QuotaSettler(limiter);

    @Test
    void normalStream_settlesWithUpstreamUsage() {
        Flux<StreamEvent> upstream = Flux.just(
                new StreamEvent.TextDelta("hi"),
                new StreamEvent.UsageUpdate(new Usage(10, 5)),
                new StreamEvent.Finished(FinishReason.STOP, "STOP"));

        StepVerifier.create(settler.meter(TENANT, "gemini-x", INPUT, MAX_TOKENS, () -> upstream))
                .expectNextCount(3)
                .verifyComplete();

        assertEquals(List.of(RESERVED - 15), limiter.adjusts); // 退回 预扣 - 实际
    }

    @Test
    void cancelMidStream_settlesOnceByEstimate() {
        Flux<StreamEvent> upstream = Flux.concat(
                Flux.just(new StreamEvent.TextDelta("hello world")),
                Flux.never()); // 上游还在生成，客户端断开

        StepVerifier.create(settler.meter(TENANT, "gemini-x", INPUT, MAX_TOKENS, () -> upstream))
                .expectNextCount(1)
                .thenCancel()
                .verify();

        int estimated = INPUT + TokenEstimator.o200k("hello world");
        assertEquals(List.of(RESERVED - estimated), limiter.adjusts); // 只结算一次，按已发出的量
    }

    @Test
    void errorBeforeFirstByte_refundsAll() {
        Flux<StreamEvent> upstream = Flux.error(new RuntimeException("connect refused"));

        StepVerifier.create(settler.meter(TENANT, "gemini-x", INPUT, MAX_TOKENS, () -> upstream))
                .verifyErrorMessage("connect refused"); // 错误照常抛给 GatewayExceptionHandler

        assertEquals(List.of(RESERVED), limiter.adjusts);
    }

    @Test
    void reserveRejected_neverCallsUpstream() {
        limiter.reject = true;
        AtomicBoolean called = new AtomicBoolean();

        StepVerifier.create(settler.meter(TENANT, "gemini-x", INPUT, MAX_TOKENS, () -> {
                    called.set(true);
                    return Flux.empty();
                }))
                .verifyError(QuotaExceededException.class);

        assertFalse(called.get());
        assertTrue(limiter.adjusts.isEmpty()); // 没预扣成功，也就不结算
    }
}

