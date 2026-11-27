package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FailoverClientTest {

    private static final ChatRequest REQUEST = ChatRequest.ofPrompt("flash", "hi", 16);
    private static final StreamEvent HELLO = new StreamEvent.TextDelta("hello");
    private static final StreamEvent DONE = new StreamEvent.Finished(FinishReason.STOP, "stop");

    /** 只用到 providerName 的假厂商 */
    record FakeAdapter(String providerName) implements ProtocolAdapter {
        @Override
        public UpstreamRequest buildRequest(ChatRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EventTranslator newTranslator() {
            throw new UnsupportedOperationException();
        }
    }

    /** 可以手动拨动的时钟，用来测冷却到期 */
    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-11-16T00:00:00Z");

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private final Candidate a = new Candidate(new FakeAdapter("a"), "model-a");
    private final Candidate b = new Candidate(new FakeAdapter("b"), "model-b");
    private final List<String> calls = new CopyOnWriteArrayList<>();
    private final MutableClock clock = new MutableClock();

    /** behavior：按厂商名返回这一次调用的结果，并记录调用顺序 */
    private FailoverClient buildClient(Function<String, Flux<StreamEvent>> behavior) {
        //把behavior 作为Upstram的一个动作，放入upstream, 主要动作：把模型provider添加到calls
        //按照behavior提前规定好的过程，处理provider

        FailoverClient.Upstream upstream = (adapter, request, timeout) -> Flux.defer(() -> {
            calls.add(adapter.providerName());
            return behavior.apply(adapter.providerName());
        });
        //返回一个client
        return new FailoverClient(upstream, new CooldownRegistry(clock, Duration.ofSeconds(30)), Duration.ofSeconds(15), clock, Duration.ofSeconds(5));
    }

    private static UpstreamHttpException http(int status, String retryAfter) {
        return new UpstreamHttpException("x", status, retryAfter, "{\"error\":{\"message\":\"boom\"}}");
    }

    @Test
    void preFirstByte503_retriesOnceThenSwitches() {
        FailoverClient client = buildClient(p -> p.equals("a")
                ? Flux.error(http(503, null))
                : Flux.just(HELLO, DONE));

        StepVerifier.create(client.streamWithFailover(List.of(a, b), REQUEST))
                .expectNext(HELLO, DONE)
                .verifyComplete();
        assertEquals(List.of("a", "a", "b"), calls);
    }

    @Test
    void errorAfterFirstByte_doesNotSwitch() {
        // 首字节后的错误在 StreamingClient 里已经是 StreamError 事件，FailoverClient 不应切换
        StreamEvent broken = new StreamEvent.StreamError("connection reset");

        //传入behavior，behvior装配到upstream， 同时利用upstream创建client
        FailoverClient client = buildClient(p -> Flux.just(HELLO, broken));

        StepVerifier.create(client.streamWithFailover(List.of(a, b), REQUEST))
                .expectNext(HELLO, broken)
                .verifyComplete();
        assertEquals(List.of("a"), calls);
    }

    @Test
    void rateLimited_skippedDuringCooldown_andSelectedAgainAfterExpiry() {
        boolean[] aLimited = {true};
        FailoverClient client = buildClient(p -> p.equals("a") && aLimited[0]
                ? Flux.error(http(429, "10"))
                : Flux.just(HELLO, DONE));

        // 第 1 次：a 返回 429 → 记冷却，切到 b
        StepVerifier.create(client.streamWithFailover(List.of(a, b), REQUEST)).expectNext(HELLO, DONE).verifyComplete();
        assertEquals(List.of("a", "b"), calls);

        // 冷却中：直接跳过 a
        calls.clear();
        aLimited[0] = false;
        clock.advance(Duration.ofSeconds(5));
        StepVerifier.create(client.streamWithFailover(List.of(a, b), REQUEST)).expectNext(HELLO, DONE).verifyComplete();
        assertEquals(List.of("b"), calls);

        // 冷却到期：又会先选 a
        calls.clear();
        clock.advance(Duration.ofSeconds(6));
        StepVerifier.create(client.streamWithFailover(List.of(a, b), REQUEST)).expectNext(HELLO, DONE).verifyComplete();
        assertEquals(List.of("a"), calls);
    }
}