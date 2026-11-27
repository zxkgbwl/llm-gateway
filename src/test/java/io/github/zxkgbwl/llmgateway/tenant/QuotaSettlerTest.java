package io.github.zxkgbwl.llmgateway.tenant;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.common.RequestTrace;
import io.github.zxkgbwl.llmgateway.cost.PriceTable;
import io.github.zxkgbwl.llmgateway.cost.PricingProperties;
import io.github.zxkgbwl.llmgateway.provider.*;
import io.github.zxkgbwl.llmgateway.quota.QuotaExceededException;
import io.github.zxkgbwl.llmgateway.quota.QuotaSettler;
import io.github.zxkgbwl.llmgateway.quota.TpmLimiter;
import io.github.zxkgbwl.llmgateway.token.TokenEstimator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
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
        boolean down;

        @Override
        public Mono<Void> reserve(Tenant tenant, int tokens) {
            if (down) return Mono.error(new RuntimeException("redis down"));
            return reject ? Mono.error(new QuotaExceededException(3, 0)) : Mono.empty();
        }

        @Override
        public Mono<Void> adjust(Tenant tenant, int delta) {
            return Mono.fromRunnable(() -> adjusts.add(delta));
        }
    }

    record StubAdapter(String providerName) implements ProtocolAdapter {
        public UpstreamRequest buildRequest(ChatRequest r) { throw new UnsupportedOperationException(); }
        public EventTranslator newTranslator() { throw new UnsupportedOperationException(); }
    }
    private final FakeLimiter limiter = new FakeLimiter();
    // 示例价格：输入 $1/M，输出 $4/M，缓存读取 $0.1/M
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final Candidate cand = new Candidate(new StubAdapter("p"), "m");
    private final QuotaSettler pricedSettler = new QuotaSettler(limiter,
            new PriceTable(List.of(new PricingProperties.ModelPrice("p", "m",
                    new BigDecimal("1"), new BigDecimal("4"), new BigDecimal("0.1")))),
            registry);

    private RequestTrace servedTrace() {
        RequestTrace trace = RequestTrace.start();
        trace.onAttempt(cand);
        return trace;
    }


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


    @Test
    void redisDown_failOpenAndChargeActualOnSettle() {
        limiter.down = true;
        Flux<StreamEvent> upstream = Flux.just(
                new StreamEvent.TextDelta("hi"),
                new StreamEvent.UsageUpdate(new Usage(10, 5)),
                new StreamEvent.Finished(FinishReason.STOP, "STOP"));

        StepVerifier.create(settler.meter(TENANT, "gemini-x", INPUT, MAX_TOKENS, () -> upstream))
                .expectNextCount(3)
                .verifyComplete();

        assertEquals(List.of(-15), limiter.adjusts); // 没预扣过，结算时补扣实际用量
    }

    @Test
    void usageSettlement_costIncludesCacheDiscount() {
        Flux<StreamEvent> upstream = Flux.just(
                new StreamEvent.TextDelta("hi"),
                new StreamEvent.UsageUpdate(new Usage(1000, 500, 400)), // 输入 1000，其中 400 命中缓存
                new StreamEvent.Finished(FinishReason.STOP, "STOP"));

        StepVerifier.create(pricedSettler.meter(TENANT, "flash", INPUT, MAX_TOKENS, servedTrace(), () -> upstream))
                .expectNextCount(3)
                .verifyComplete();

        // 600×1 + 400×0.1 + 500×4 = 2640 微美元
        double usd = registry.get("gateway.cost").tag("estimated", "false").counter().count();
        assertEquals(0.002640, usd, 1e-9);
    }

    @Test
    void cancelledStream_costMarkedEstimated() {
        Flux<StreamEvent> upstream = Flux.concat(
                Flux.just(new StreamEvent.TextDelta("hello world")),
                Flux.never());

        StepVerifier.create(pricedSettler.meter(TENANT, "flash", INPUT, MAX_TOKENS, servedTrace(), () -> upstream))
                .expectNextCount(1)
                .thenCancel()
                .verify();

        int output = TokenEstimator.o200k("hello world");
        double expected = (INPUT * 1 + output * 4) / 1e6; // 估算输入 + 已发出的输出
        double usd = registry.get("gateway.cost").tag("estimated", "true").counter().count();
        assertEquals(expected, usd, 1e-9);
    }
}

