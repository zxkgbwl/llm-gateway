package io.github.zxkgbwl.llmgateway.quota;


import ch.qos.logback.core.subst.Token;
import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent;
import io.github.zxkgbwl.llmgateway.provider.Usage;
import io.github.zxkgbwl.llmgateway.tenant.Tenant;
import io.github.zxkgbwl.llmgateway.token.TokenEstimator;
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
 * 给一次流式调用套上“预扣 → 转发 → 结算”。
 *
 * 用 Flux.usingWhen 而不是 doFinally：完成、出错、取消三条路径分开写；
 * 结算是异步的 Redis 调用，usingWhen 会订阅它，doFinally 里只能自己 subscribe。
 */
public class QuotaSettler {
    private static final Logger log = LoggerFactory.getLogger(QuotaSettler.class);

    private static final Logger settlementLog = LoggerFactory.getLogger("settlement");

    private static final int PRE_MESSAGE_OVERHEAD = 4;

    public enum Source{USAGE, ESTIMATED, REFUNDED}

    enum Outcome {COMPLETE, ERROR, CANCEL}

    record Settlement(Source source, int actual) {}

    private final TpmLimiter limiter;

    public QuotaSettler(TpmLimiter limiter) {
        this.limiter = limiter;
    }

    public static int estimateInput(ChatRequest request, double factor) {
        int raw = request.messages().stream()
                .mapToInt(m -> TokenEstimator.o200k(m.content()) + PRE_MESSAGE_OVERHEAD)
                .sum();

        return (int) Math.ceil(raw * factor);
    }

    public Flux<StreamEvent> meter(Tenant tenant, String model, int estimatedInput, int maxTokens,
                                   Supplier<Flux<StreamEvent>> upstream) {
        return Flux.defer(()->{
            int reserved = estimatedInput + maxTokens;
            if (reserved > tenant.tpm()) {
                return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Request needs " + reserved + " tokens, above tenant TPM " + tenant.tpm()
                        + "; lower max_tokens or shorten the prompt"));
            }

            return Flux.usingWhen(
                    limiter.reserve(tenant, reserved)
                            .thenReturn(new Meter(tenant, model, estimatedInput, reserved)),
                    meter -> upstream.get().doOnNext(meter::observe),
                    meter -> settle(meter, Outcome.COMPLETE),
                    (meter, error) -> settle(meter, Outcome.ERROR),
                    meter -> settle(meter, Outcome.CANCEL));



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
            settlementLog.info("requestId={} tenant={} model={} outcome={} source={} reserved={} actual={} delta={} ratio={} estInput={} usage={}",
                    meter.requestId, meter.tenant.id(), meter.model, outcome, s.source(),
                    meter.reserved, s.actual(), delta,
                    s.actual() > 0 ? String.format("%.2f", (double) meter.reserved / s.actual()) : "n/a",
                    meter.estimatedInput, meter.lastUsage);
            // 结算失败只记日志：不能让 Redis 的问题把已经发完的流变成错误
            return limiter.adjust(meter.tenant, delta)
                    .onErrorResume(e -> {
                        log.warn("Settlement failed, requestId={} delta={}", meter.requestId, delta, e);
                        return Mono.empty();
                    });
        });

    }

    static final class Meter {
        final String requestId = UUID.randomUUID().toString();
        final Tenant tenant;
        final String model;
        final int estimatedInput;
        final int reserved;
        final AtomicBoolean settled = new AtomicBoolean();
        final AtomicInteger observeOutput = new AtomicInteger();
        volatile Usage lastUsage;
        volatile boolean started;
        volatile boolean finished;

        Meter (Tenant tenant, String model, int estimatedInput, int reserved) {
            this.tenant = tenant;
            this.model = model;
            this.estimatedInput = estimatedInput;
            this.reserved = reserved;
        }

        void observe(StreamEvent event) {
            started = true;
            switch(event) {
                case StreamEvent.TextDelta d -> observeOutput.addAndGet(TokenEstimator.o200k(d.text()));
                case StreamEvent.UsageUpdate u -> lastUsage = u.usage();
                case StreamEvent.Finished f -> finished = true;
                case StreamEvent.StreamError e ->{}
            }
        }

        Settlement compute(Outcome outcome) {
            if (outcome == Outcome.ERROR && !started) {
                return new Settlement(Source.REFUNDED, 0);

            }

            Usage usage = lastUsage;
            if (finished && usage != null) {
                return new Settlement(Source.USAGE, usage.totalTokens());
            }

            int input = (usage != null && usage.inputTokens() > 0) ? usage.inputTokens() : estimatedInput;
            int output = Math.max(usage != null ? usage.outputTokens() : 0, observeOutput.get());
            return new Settlement(Source.ESTIMATED, input + output);

        }







    }


}
