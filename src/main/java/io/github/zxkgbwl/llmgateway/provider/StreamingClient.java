package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.sse.SseDecoder;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamingClient {
    private static final StreamEvent.StreamError TRUNCATED =
            new StreamEvent.StreamError("Stream ended without a finish signal; response is truncated!");

    private final WebClient webClient;;
    private final SseDecoder decoder;

    /** 空闲超时：相邻两次收到上游原始字节的最大间隔；null 表示不限 */
    private final Duration idleTimeout;

    // WebClient 和解码器都在构造时注入
    public StreamingClient(WebClient webClient, SseDecoder decoder) {
        this(webClient, decoder, null);
    }

    // WebClient、解码器和空闲超时都在构造时注入
    public StreamingClient(WebClient webClient, SseDecoder decoder, Duration idleTimeout) {
        this.webClient = webClient;
        this.decoder = decoder;
        this.idleTimeout = idleTimeout;
    }

    /** 不限首字节时间（playground 用） */
    public Flux<StreamEvent> stream(ProtocolAdapter adapter, ChatRequest request) {
        return stream(adapter, request, null);
    }

    public Flux<StreamEvent> stream(ProtocolAdapter adapter, ChatRequest request, Duration firstByteTimeout) {
        return Flux.defer( () -> {
            UpstreamRequest upstream = adapter.buildRequest(request);

            Flux<DataBuffer> body = webClient.post()
                    .uri(upstream.uri())
                    .headers(h->upstream.headers().forEach(h::set))
                    .bodyValue(upstream.jsonBody())
                    .exchangeToFlux(response-> {
                        if (!response.statusCode().is2xxSuccessful()) {
                            return response.bodyToMono(String.class)
                                    .defaultIfEmpty("")
                                    .flatMapMany(errorBody -> Flux.error(new UpstreamHttpException(
                                            adapter.providerName(),
                                            response.statusCode().value(),
                                            response.headers().asHttpHeaders().getFirst("retry-after"),
                                            errorBody)));
                        }
                        return response.bodyToFlux(DataBuffer.class);
                    });

            if (firstByteTimeout != null) {
                //firstByteTimeout 时间内没有数据，就超时，如果第一个数据没有超时，后续不太统计超时（Mono.never()）
                body = body.timeout(Mono.delay(firstByteTimeout), buffer -> Mono.never());

            }

            return toEvents(body, adapter.newTranslator(), decoder);

        });

    }

    /**
     * 两个超时都按上游原始字节（DataBuffer）计时，而不是按 StreamEvent：
     * 思考片段会被翻译器过滤掉、不产生事件，按事件计时会把“正在思考”误判成卡死。
     * <p>首字节超时触发时还没收到首字节 → 以 onError 抛出，FailoverClient 可以重试或切换；
     * 空闲超时触发时已经收到首字节 → 在 toEvents 里变成 StreamError，不切换。
     * 超时都会取消订阅，连接随之关闭。
     */
    static Flux<DataBuffer> withTimeouts(Flux<DataBuffer> body, Duration firstByte, Duration idle) {
        if (firstByte == null && idle == null) {
            return body;
        }
        Mono<Long> first = firstByte != null ? Mono.delay(firstByte) : Mono.<Long>never();
        return body.timeout(first, buffer -> idle != null ? Mono.delay(idle) : Mono.<Long>never());
    }


    static Flux<StreamEvent> toEvents(Flux<DataBuffer> body, EventTranslator translator, SseDecoder decoder) {
      return Flux.defer(()-> {
          //是否已经收到首字节：用来区分“流开始前”和“流开始后”的错误
          AtomicBoolean started = new AtomicBoolean();
          return decoder.decode(body.doOnNext(buffer-> started.set(true)))
                  .concatMapIterable(translator::translate)
                  //流开始后的错误（断连、半行、翻译失败）转成 StreamError 事件；
                  //流开始前的错误（状态码非 2xx、连接失败）照常抛给调用方，后面据此决定重试或切换厂商
                  .onErrorResume(e-> started.get(), e->Flux.just(new StreamEvent.StreamError(describe(e))))
                  // 上游正常结束却没给出结束信号 → 截断。
                  // 如果前面已经有终止事件，takeUntil 会在它之后取消订阅，TRUNCATED 不会发出
                  .concatWith(Mono.just(TRUNCATED))
                  .takeUntil(StreamEvent::isTerminal);
      });
    }

    private static String describe(Throwable e) {
        return e.getClass().getSimpleName() + ": " + e.getMessage();
    }
}
