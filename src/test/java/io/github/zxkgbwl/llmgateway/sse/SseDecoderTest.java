package io.github.zxkgbwl.llmgateway.sse;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;

import static io.github.zxkgbwl.llmgateway.provider.CaptureReplayTest.chunked;

public class SseDecoderTest {
    private final SseDecoder decoder = new SseDecoder(line -> {});

    // 切成单字节：同时覆盖半行、半个汉字、\r\n、注释行、多行 data
    @Test
    void decodesCorrectly_whenSplitIntoSingleBytes() {
        String sse = ": 注释\r\nevent: delta\r\ndata: {\"text\":\"你好\"}\r\n\r\n"
                + "data: 第一行\ndata: 第二行\n\n";
        byte[] bytes = sse.getBytes(StandardCharsets.UTF_8);

        StepVerifier.create(decoder.decode(chunked(bytes, 1)))
                .expectNext(new SseEvent("delta", "{\"text\":\"你好\"}"))
                .expectNext(new SseEvent(null, "第一行\n第二行"))
                .verifyComplete();
    }

    // 流停在半行：报错，由 StreamingClient 转成 StreamError
    @Test
    void endingMidLine_isError() {
        byte[] bytes = "data: {\"te".getBytes(StandardCharsets.UTF_8);
        StepVerifier.create(decoder.decode(chunked(bytes, 3)))
                .verifyError(IllegalStateException.class);
    }
}
