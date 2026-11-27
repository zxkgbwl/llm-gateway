package io.github.zxkgbwl.llmgateway.quota;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.common.RequestTrace;
import io.github.zxkgbwl.llmgateway.cost.PriceTable;
import io.github.zxkgbwl.llmgateway.events.SettlementEvent;
import io.github.zxkgbwl.llmgateway.events.SettlementListener;
import io.github.zxkgbwl.llmgateway.provider.Candidate;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent;
import io.github.zxkgbwl.llmgateway.provider.Usage;
import io.github.zxkgbwl.llmgateway.tenant.Tenant;
import io.github.zxkgbwl.llmgateway.token.TokenEstimator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 给一次流式调用套上“预扣 → 转发 → 结算（含成本）”。
 *
 * 用 Flux.usingWhen 而不是 doFinally：完成、出错、取消三条路径分开写；
 * 结算是异步的 Redis 调用，usingWhen 会订阅它，doFinally 里只能自己 subscribe。
 */
public class QuotaSettler {
    private static final Logger log = LoggerFactory.getLogger(QuotaSettler.class);

    private static final Logger settlementLog = LoggerFactory.getLogger("settlement");

    private static final int PRE_MESSAGE_OVERHEAD = 4;

    public enum Source {USAGE, ESTIMATED, REFUNDED}

    enum Outcome {COMPLETE, ERROR, CANCEL}

    /** 结算结果：token 拆成输入 / 输出 / 缓存读取，算成本要用 */
    record Settlement(Source source, int input, int output, int cacheRead) {
        int actual() { return input + output; }
    }

    private final TpmLimiter limiter;
    private final PriceTable prices;
    private final MeterRegistry registry;
    private final SettlementListener listener;

    /** 不算成本的调用方（测试）用 */
    public QuotaSettler(TpmLimiter limiter) {
        this(limiter, PriceTable.EMPTY, new SimpleMeterRegistry(), SettlementListener.NONE);
    }

    public QuotaSettler(TpmLimiter limiter, PriceTable prices, MeterRegistry registry) {

        this(limiter, prices, registry, SettlementListener.NONE);

    }

    public QuotaSettler(TpmLimiter limiter, PriceTable prices, MeterRegistry registry, SettlementListener listener) {
        this.limiter = limiter;
        this.prices = prices;
        this.registry = registry;
        this.listener = listener;
    }

    public static int estimateInput(ChatRequest request, double factor) {
        int raw = request.messages().stream()
                .mapToInt(m -> TokenEstimator.o200k(m.content()) + PRE_MESSAGE_OVERHEAD)
                .sum();
        return (int) Math.ceil(raw * factor);
    }

    /** 旧签名：不关心实际候选的调用方（测试）用 */
    public Flux<StreamEvent> meter(Tenant tenant, String model, int estimatedInput, int maxTokens,
                                   Supplier<Flux<StreamEvent>> upstream) {
        return meter(tenant, model, estimatedInput, maxTokens, RequestTrace.start(), upstream);
    }

    public Flux<StreamEvent> meter(Tenant tenant, String model, int estimatedInput, int maxTokens,
                                   RequestTrace trace, Supplier<Flux<StreamEvent>> upstream) {
        return Flux.defer(() -> {
            int reserved = estimatedInput + maxTokens;
            if (reserved > tenant.tpm()) {
                return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Request needs " + reserved + " tokens, above tenant TPM " + tenant.tpm()
                                + "; lower max_tokens or shorten the prompt"));
            }

            return Flux.usingWhen(
                    reserveOrFailOpen(tenant, reserved)
                            // fail-open 时没有真正预扣，按 0 记：结算时 delta = -actual，Redis 恢复了就补扣
                            .map(ok -> new Meter(tenant, model, estimatedInput, ok ? reserved : 0, trace)),
                    meter -> upstream.get().doOnNext(meter::observe),
                    meter -> settle(meter, Outcome.COMPLETE),
                    (meter, error) -> settle(meter, Outcome.ERROR),
                    meter -> settle(meter, Outcome.CANCEL));
        });
    }

    /** TPM 预扣：超额照常抛 429；Redis 本身出问题时放行（fail-open），返回 false 表示没有真正预扣。 */
    private Mono<Boolean> reserveOrFailOpen(Tenant tenant, int reserved) {
        return limiter.reserve(tenant, reserved)
                .thenReturn(true)
                .onErrorResume(e -> !(e instanceof QuotaExceededException), e -> {
                    // 只打一行，不打堆栈：Redis 宕机时每个请求都会走到这里
                    log.warn("TPM 预扣失败，fail-open 放行：tenant={} reserved={} cause={}",
                            tenant.id(), reserved, e.toString());
                    return Mono.just(false);
                });
    }

    private Mono<Void> settle(Meter meter, Outcome outcome) {
        return Mono.defer(() -> {
            // 取消和完成可能同时到达，保证只结算一次
            if (!meter.settled.compareAndSet(false, true)) {
                return Mono.empty();
            }
            Settlement s = meter.compute(outcome);
            int delta = meter.reserved - s.actual();
            Long cost = cost(meter.trace.servedBy(), s);
            boolean estimated = s.source() == Source.ESTIMATED;

            settlementLog.info("requestId={} tenant={} model={} provider={} outcome={} source={} reserved={} actual={} delta={} ratio={} estInput={} usage={} costNanoUsd={} costEstimated={}",
                    meter.requestId, meter.tenant.id(), meter.model, meter.trace.providerKey(), outcome, s.source(),
                    meter.reserved, s.actual(), delta,
                    s.actual() > 0 ? String.format("%.2f", (double) meter.reserved / s.actual()) : "n/a",
                    meter.estimatedInput, meter.lastUsage, cost, estimated);

            // 事件发布失败不能影响结算
            try {
                listener.onSettled(new SettlementEvent(meter.requestId, System.currentTimeMillis(),
                        meter.tenant.id(), meter.model, meter.trace.providerKey(), outcome.name(), s.source().name(),
                        meter.reserved, s.input(), s.output(), s.cacheRead(), cost, estimated));
            } catch (RuntimeException e) {
                log.warn("结算事件回调失败：requestId={}", meter.requestId, e);
            }

            // 给看板用；double 有精度误差，准确金额以结算日志 / Kafka 事件为准
            if (cost != null && cost > 0) {
                Counter.builder("gateway.cost").baseUnit("usd")
                        .tags("tenant", meter.tenant.id(), "provider", meter.trace.providerKey(),
                                "estimated", String.valueOf(estimated))
                        .register(registry)
                        .increment(cost / 1e9);
            }

            // 结算失败只记日志：不能让 Redis 的问题把已经发完的流变成错误
            return limiter.adjust(meter.tenant, delta)
                    .onErrorResume(e -> {
                        log.warn("Settlement failed, requestId={} delta={}", meter.requestId, delta, e);
                        return Mono.empty();
                    });
        });
    }

    /** 成本（纳美元）。退款为 0；没打到上游或没配价格返回 null（日志里显示 null，即未计价）。 */
    private Long cost(Candidate served, Settlement s) {
        if (s.source() == Source.REFUNDED) return 0L;
        if (served == null) return null;
        return prices.find(served)
                .map(p -> p.costNanoUsd(s.input(), s.output(), s.cacheRead()))
                .orElseGet(() -> {
                    log.warn("没有价格配置，成本未计：candidate={}", served.key());
                    return null;
                });
    }

    static final class Meter {
        final String requestId = UUID.randomUUID().toString();
        final Tenant tenant;
        final String model;
        final int estimatedInput;
        final int reserved;
        final RequestTrace trace;
        final AtomicBoolean settled = new AtomicBoolean();
        final AtomicInteger observeOutput = new AtomicInteger();
        volatile Usage lastUsage;
        volatile boolean started;
        volatile boolean finished;

        Meter(Tenant tenant, String model, int estimatedInput, int reserved, RequestTrace trace) {
            this.tenant = tenant;
            this.model = model;
            this.estimatedInput = estimatedInput;
            this.reserved = reserved;
            this.trace = trace;
        }

        void observe(StreamEvent event) {
            started = true;
            switch (event) {
                case StreamEvent.TextDelta d -> observeOutput.addAndGet(TokenEstimator.o200k(d.text()));
                case StreamEvent.UsageUpdate u -> lastUsage = u.usage();
                case StreamEvent.Finished f -> finished = true;
                case StreamEvent.StreamError e -> { }
            }
        }

        Settlement compute(Outcome outcome) {
            if (outcome == Outcome.ERROR && !started) {
                return new Settlement(Source.REFUNDED, 0, 0, 0);
            }

            Usage usage = lastUsage;
            if (finished && usage != null) {
                return new Settlement(Source.USAGE, usage.inputTokens(), usage.outputTokens(), usage.cacheReadTokens());
            }

            int input = (usage != null && usage.inputTokens() > 0) ? usage.inputTokens() : estimatedInput;
            int output = Math.max(usage != null ? usage.outputTokens() : 0, observeOutput.get());
            int cacheRead = usage != null ? usage.cacheReadTokens() : 0;
            return new Settlement(Source.ESTIMATED, input, output, cacheRead);
        }
    }
}