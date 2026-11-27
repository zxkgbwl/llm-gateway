package io.github.zxkgbwl.llmgateway.metrics;

import io.github.zxkgbwl.llmgateway.common.RequestTrace;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent;
import io.github.zxkgbwl.llmgateway.provider.Usage;
import io.github.zxkgbwl.llmgateway.token.TokenEstimator;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 流式请求的延迟指标：TTFT、上游 TTFT、TPOT、tok/s。
 *
 * <p>计时点：请求开始 → 第一个 TextDelta（和 FirstTokenTimeout 同一个点）→ 最后一个 TextDelta → Finished。
 * 用 Finished 事件而不是 onComplete 作为结束点：onComplete 要等结算的 Redis 调用。
 *
 * <p>只有正常完成的流记延迟；所有请求按 outcome（ok / error / cancel）计数。
 * 请求开始时间、实际候选、时钟都来自 RequestTrace。
 */
public class StreamMetrics {
    private final MeterRegistry registry;

    public StreamMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** 每次订阅新建一个（有状态） */
    public Recorder start(String tenantId, String model, RequestTrace trace) {
        return new Recorder(Tags.of("tenant", tenantId, "model", model), trace);
    }

    private Timer timer(String name, Tags tags, Duration min, Duration max) {
        return Timer.builder(name).tags(tags)
                .publishPercentileHistogram()
                .minimumExpectedValue(min)
                .maximumExpectedValue(max)
                .register(registry);
    }

    public final class Recorder {
        private final Tags baseTags;
        private final RequestTrace trace;
        private final AtomicInteger estimatedOutput = new AtomicInteger();
        private final AtomicBoolean recorded = new AtomicBoolean();
        private volatile long firstTextNanos = -1;
        private volatile long lastTextNanos;
        private volatile long finishedNanos = -1;
        private volatile Usage usage;

        private Recorder(Tags baseTags, RequestTrace trace) {
            this.baseTags = baseTags;
            this.trace = trace;
        }

        public Flux<StreamEvent> observe(Flux<StreamEvent> events) {
            return events.doOnNext(this::onEvent).doFinally(this::record);
        }

        private void onEvent(StreamEvent event) {
            long now = trace.now();
            switch (event) {
                case StreamEvent.TextDelta d -> {
                    if (firstTextNanos < 0) firstTextNanos = now;
                    lastTextNanos = now;
                    estimatedOutput.addAndGet(TokenEstimator.o200k(d.text()));
                }
                case StreamEvent.UsageUpdate u -> usage = u.usage();
                case StreamEvent.Finished f -> finishedNanos = now;
                case StreamEvent.StreamError e -> { }
            }
        }

        private void record(SignalType signal) {
            if (!recorded.compareAndSet(false, true)) return;
            String outcome = finishedNanos >= 0 ? "ok" : signal == SignalType.CANCEL ? "cancel" : "error";
            Tags tags = baseTags.and("provider", trace.providerKey());
            registry.counter("gateway.requests", tags.and("outcome", outcome)).increment();
            if (!"ok".equals(outcome) || firstTextNanos < 0) return;

            // 客户端视角：含排队、预扣、重试和切换
            timer("gateway.ttft", tags, Duration.ofMillis(10), Duration.ofSeconds(60))
                    .record(firstTextNanos - trace.startNanos(), TimeUnit.NANOSECONDS);
            // 上游视角：只算实际服务的那次尝试，用来区分“厂商慢”和“网关 / 切换慢”
            timer("gateway.upstream.ttft", tags, Duration.ofMillis(10), Duration.ofSeconds(60))
                    .record(firstTextNanos - trace.attemptStartNanos(), TimeUnit.NANOSECONDS);

            // 一个 chunk 可能含多个 token，所以用总时长 / token 数，不用 chunk 间隔
            int output = outputTokens();
            if (output > 1 && lastTextNanos > firstTextNanos) {
                timer("gateway.tpot", tags, Duration.ofMillis(1), Duration.ofSeconds(1))
                        .record((lastTextNanos - firstTextNanos) / (output - 1), TimeUnit.NANOSECONDS);
            }
            long genNanos = finishedNanos - firstTextNanos;
            if (genNanos > 0) {
                DistributionSummary.builder("gateway.throughput").baseUnit("tokens_per_second").tags(tags)
                        .publishPercentileHistogram()
                        .minimumExpectedValue(1.0).maximumExpectedValue(1000.0)
                        .register(registry)
                        .record(output * 1e9 / genNanos);
            }
        }

        /** 优先用 usage；没有时用已发出文本的估算。usage 的输出含思考 token，会让推理模型的 tok/s 偏高。 */
        private int outputTokens() {
            Usage u = usage;
            return (u != null && u.outputTokens() > 0) ? u.outputTokens() : estimatedOutput.get();
        }
    }
}