package io.github.zxkgbwl.llmgateway.provider;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/** 用虚拟时间测超时，不用真等 */
class TimeoutsTest {

    private static final Duration FIRST_TOKEN = Duration.ofSeconds(60);
    private static final StreamEvent TEXT = new StreamEvent.TextDelta("hi");
    private static final StreamEvent USAGE = new StreamEvent.UsageUpdate(new Usage(10, 0));
    private static final StreamEvent DONE = new StreamEvent.Finished(FinishReason.STOP, "stop");


    //即使收到了 Usage，只要没有收到正文 Token，首 Token 超时仍然有效。
    @Test
    void firstToken_timesOutWhenNoTextArrives() {
        // 只有 usage、没有正文，然后上游一直不说话
        StepVerifier.withVirtualTime(() -> FirstTokenTimeout.apply(
                        Flux.concat(Flux.just(USAGE), Flux.never()), FIRST_TOKEN))
                .expectNext(USAGE)
                .thenAwait(FIRST_TOKEN)
                .expectNextMatches(e -> e instanceof StreamEvent.StreamError)
                .verifyComplete();
    }

    @Test
    void firstToken_notTriggeredOnceTextArrived() {
        // 第一个字已经到了，之后再慢也不归首 token 超时管（归空闲超时）
        StepVerifier.withVirtualTime(() -> FirstTokenTimeout.apply(
                        Flux.concat(Flux.just(TEXT), Mono.delay(Duration.ofSeconds(90)).thenReturn(DONE)),
                        FIRST_TOKEN))
                .expectNext(TEXT)
                .thenAwait(Duration.ofSeconds(90))
                .expectNext(DONE)
                .verifyComplete();
    }

    @Test
    void idle_timesOutAfterFirstByte() {
        DataBuffer chunk = DefaultDataBufferFactory.sharedInstance
                .wrap("data: x\n\n".getBytes(StandardCharsets.UTF_8));
        // 首字节到了，之后上游不再发任何字节
        StepVerifier.withVirtualTime(() -> StreamingClient.withTimeouts(
                        Flux.concat(Flux.just(chunk), Flux.never()),
                        Duration.ofSeconds(15), Duration.ofSeconds(30)))
                .expectNextCount(1)
                .thenAwait(Duration.ofSeconds(30))
                .expectError(TimeoutException.class)
                .verify();
    }
}