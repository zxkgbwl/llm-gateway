package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.sse.SseDecoder;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;

import java.util.concurrent.atomic.AtomicBoolean;

public class StreamingClient {
    private static final StreamEvent.StreamError TRUNCATED =
            new StreamEvent.StreamError("Stream ended without a finish signal; response is truncated!");

    private final WebClient webClient;;
    private final SseDecoder decoder;

    // WebClient 和解码器都在构造时注入
    public StreamingClient(WebClient webClient, SseDecoder decoder) {
        this.webClient = webClient;
        this.decoder = decoder;
    }

    public Flux<StreamEvent> stream(ProtocolAdapter adapter, ChatRequest request) {
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

            return toEvents(body, adapter.newTranslator(), decoder);


        });
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
