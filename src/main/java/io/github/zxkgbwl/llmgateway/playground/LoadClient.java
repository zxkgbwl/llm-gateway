
package io.github.zxkgbwl.llmgateway.playground;

import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并发冒烟测试：同时向网关发起 N 个流式请求，统计成功和失败的数量。
 * 单独作为一个进程运行，这样它自己的线程不会混进网关的统计数据里。
 */
public class LoadClient {

    public static void main(String[] args) {
        int streams = args.length > 0 ? Integer.parseInt(args[0]) : 1000;

        // 客户端自己的连接池要能放下 N 个并发连接，否则瓶颈会出在压测客户端上
        ConnectionProvider pool = ConnectionProvider.builder("load")
                .maxConnections(streams)
                .pendingAcquireMaxCount(-1)
                .build();
        WebClient client = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create(pool)))
                .build();

        String body = """
                {"model":"fake-model","stream":true,"max_tokens":100,
                 "messages":[{"role":"user","content":"hi"}]}""";

        AtomicInteger failed = new AtomicInteger();
        long start = System.nanoTime();

        List<Long> eventCounts = Flux.range(0, streams)
                .delayElements(Duration.ofMillis(5))
                .flatMap(i -> client.post()
                        .uri("http://localhost:8080/v1/chat/completions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization", "Bearer vk-demo-123")
                        .bodyValue(body)
                        .retrieve()
                        .bodyToFlux(String.class)
                        .count()
                        .onErrorResume(e -> {
                            // 只打印前几个错误，避免刷屏
                            if (failed.incrementAndGet() <= 5) {
                                System.out.println("失败：" + e);
                            }
                            return Mono.just(-1L);
                        }), streams) // 并发度等于流的数量：所有请求同时发出
                .collectList()
                .block();

        long ok = eventCounts.stream().filter(n -> n > 0).count();
        System.out.printf("流数：%d，成功：%d，失败：%d，耗时：%.1f 秒%n",
                streams, ok, failed.get(), (System.nanoTime() - start) / 1e9);
    }
}