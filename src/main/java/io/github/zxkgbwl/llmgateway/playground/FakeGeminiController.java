package io.github.zxkgbwl.llmgateway.playground;

import org.springframework.context.annotation.Profile;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * 压测用的假 Gemini 上游：只在 loadtest profile 下启用，不影响正常运行。
 */
@Profile("loadtest")
@RestController
public class FakeGeminiController {

    private static final String CHUNK =
            "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"tok \"}],\"role\":\"model\"},\"index\":0}]}";
    private static final String LAST =
            "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],"
                    + "\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":300,\"totalTokenCount\":305}}";

    // 匹配 /v1beta/models/fake-model:streamGenerateContent?alt=sse
    @PostMapping("/v1beta/models/{spec}")
    public Flux<ServerSentEvent<String>> stream(@PathVariable("spec") String spec) {
        return Flux.interval(Duration.ofMillis(100))
                .take(300)
                .map(i -> CHUNK)
                .concatWith(Mono.just(LAST))
                .map(data -> ServerSentEvent.<String>builder(data).build());
    }
}