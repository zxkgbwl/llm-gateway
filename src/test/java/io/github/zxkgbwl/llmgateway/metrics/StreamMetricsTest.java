package io.github.zxkgbwl.llmgateway.metrics;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.common.RequestTrace;
import io.github.zxkgbwl.llmgateway.provider.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class StreamMetricsTest {
    record StubAdapter(String providerName) implements ProtocolAdapter {
        public UpstreamRequest buildRequest(ChatRequest r) { throw new UnsupportedOperationException(); }
        public EventTranslator newTranslator() { throw new UnsupportedOperationException(); }
    }

    private final Candidate cand = new Candidate(new StubAdapter("gemini"), "g");
    private final VirtualTimeScheduler vts = VirtualTimeScheduler.create();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final StreamMetrics metrics = new StreamMetrics(registry);

    private static Mono<StreamEvent> after(long ms, StreamEvent e) {
        return Mono.delay(Duration.ofMillis(ms)).map(x -> e);
    }

    @Test
    void completedStream_recordsTtftTpotThroughput() {
        StepVerifier.withVirtualTime(() -> {
                    RequestTrace trace = new RequestTrace(() -> vts.now(TimeUnit.NANOSECONDS)); // t=0
                    StreamMetrics.Recorder rec = metrics.start("t1", "flash", trace);   // t=0
                    Flux<StreamEvent> events = Flux.concat(
                            Mono.delay(Duration.ofMillis(100)).doOnNext(x -> trace.onAttempt(cand)).then(Mono.empty()), // t=100 发起尝试
                            after(400, new StreamEvent.TextDelta("a")),                     // t=500 首个 TextDelta
                            after(1000, new StreamEvent.TextDelta("b")),                    // t=1500 最后一个
                            Mono.just(new StreamEvent.UsageUpdate(new Usage(10, 11))),
                            after(500, new StreamEvent.Finished(FinishReason.STOP, "STOP"))); // t=2000
                    return rec.observe(events);
                }, () -> vts, Long.MAX_VALUE)
                .thenAwait(Duration.ofSeconds(3))
                .expectNextCount(4)
                .verifyComplete();

        assertEquals(500, registry.get("gateway.ttft").tag("provider", "gemini:g").timer().totalTime(TimeUnit.MILLISECONDS), 1e-6);
        assertEquals(400, registry.get("gateway.upstream.ttft").timer().totalTime(TimeUnit.MILLISECONDS), 1e-6);
        assertEquals(100, registry.get("gateway.tpot").timer().totalTime(TimeUnit.MILLISECONDS), 1e-6); // 1000ms / (11-1)
        assertEquals(11 / 1.5, registry.get("gateway.throughput").summary().max(), 1e-6);           // 11 token / 1.5s
        assertEquals(1, registry.get("gateway.requests").tag("outcome", "ok").counter().count());
    }

    @Test
    void streamError_countsButNoLatency() {
        RequestTrace trace = RequestTrace.start();
        StreamMetrics.Recorder rec = metrics.start("t1", "flash", trace);
        trace.onAttempt(cand);
        StepVerifier.create(rec.observe(Flux.just(
                        new StreamEvent.TextDelta("a"), new StreamEvent.StreamError("boom"))))
                .expectNextCount(2)
                .verifyComplete();

        assertEquals(1, registry.get("gateway.requests").tag("outcome", "error").counter().count());
        assertNull(registry.find("gateway.ttft").timer());
    }
}