package io.github.zxkgbwl.llmgateway.provider;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 首 token 超时：从请求开始计时，到事件流里出现第一个 TextDelta 为止。
 *
 * <p>和空闲超时的区别：上游可能一直在发思考片段（字节不断、空闲超时不会触发），却迟迟不出正文。
 * 这里限制的是“用户等多久能看到第一个字”，和第 5 周的 TTFT 用同一个计时点（第一个 TextDelta）。
 *
 * <p>包在 FailoverClient 外面，计时覆盖整个请求（含重试和切换）。超时值必须大于首字节总预算：
 * 首字节前的失败要以 onError 交给 FailoverClient 处理，不能先被这里变成 StreamError。
 */
public class FirstTokenTimeout {
    private FirstTokenTimeout() {}

    public static Flux<StreamEvent> apply(Flux<StreamEvent> events, Duration timeout) {
        return Flux.defer(()->{
            AtomicBoolean gotText = new AtomicBoolean();

            // 上游流结束（正常或出错）时通知计时器提前退出，不然流要等计时器到点才能完成
            Sinks.Empty<Void> done = Sinks.empty();

            Flux<StreamEvent> marked = events
                    .doOnNext(event -> {
                        if (event instanceof StreamEvent.TextDelta)
                        {
                        gotText.set(true);}
                    })
                    .doOnTerminate(done::tryEmitEmpty);

            Mono<StreamEvent> timer = Mono.delay(timeout)
                    .takeUntilOther(done.asMono())
                    .filter(t -> !gotText.get())
                    .<StreamEvent>map(t->new StreamEvent.StreamError(
                            "No output token within " + timeout.toSeconds() + "s"));

            return Flux.<StreamEvent>merge(marked, timer)
                    .takeUntil(StreamEvent::isTerminal);

        });
    }


}
