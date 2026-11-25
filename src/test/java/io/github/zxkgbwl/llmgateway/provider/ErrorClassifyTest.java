package io.github.zxkgbwl.llmgateway.provider;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorClassifyTest {

    private final GeminiAdapter gemini = new GeminiAdapter("https://example.com", "unused");

    @Test
    void geminiBadKey400_isProviderMisconfigured() throws IOException {
        // 第 1 周录下的真实错误体：Key 无效时 Gemini 返回 400，而不是 401
        String body = read("/captures/gemini-badkey.txt");
        assertEquals(ErrorClass.PROVIDER_MISCONFIGURED,
                gemini.classify(new UpstreamHttpException("gemini", 400, null, body)));
    }

    @Test
    void geminiOrdinary400_isClientError() {
        String body = "{\"error\":{\"code\":400,\"message\":\"Invalid value\",\"status\":\"INVALID_ARGUMENT\"}}";
        assertEquals(ErrorClass.CLIENT_ERROR,
                gemini.classify(new UpstreamHttpException("gemini", 400, null, body)));
    }

    private static String read(String path) throws IOException {
        try (InputStream in = ErrorClassifyTest.class.getResourceAsStream(path)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
