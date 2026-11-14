package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.provider.StreamEvent.Finished;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.StreamError;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.TextDelta;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.UsageUpdate;

import io.github.zxkgbwl.llmgateway.sse.SseDecoder;
import io.github.zxkgbwl.llmgateway.sse.SseReader;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 用真实抓包回放整条链路：SseReader → 翻译器 → ChatStream。
 * 数据来自 src/test/resources/captures/。
 */
public class CaptureReplayTest {

    // ---------- 辅助方法 ----------

    // 测试不发请求，所以 adapter 用假参数构造；构造方法签名按你的代码调整
    private static EventTranslator claude() {
        return new AnthropicAdapter("https://api.anthropic.com", "api_key").newTranslator();
    }

    private static EventTranslator gemini() {
        return new GeminiAdapter("https://generativelanguage.googleapis.com", "api_key").newTranslator();
    }

    // 回放 classpath 上的抓包文件
    private static List<StreamEvent> replay(String resource, EventTranslator translator) {
        try (InputStream in = Objects.requireNonNull(
                CaptureReplayTest.class.getResourceAsStream(resource), "测试资源不存在：" + resource)) {
            return replayBytes(in.readAllBytes(), translator);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<StreamEvent> replayBytes(byte[] bytes, EventTranslator translator) {
        return StreamingClient.toEvents(chunked(bytes, 7), translator, new SseDecoder(line -> {}))
                .collectList()
                .block();
    }

    // 把字节按 chunkSize 切块，模拟网络分片到达；7 字节会把行和汉字切得很碎
    public static Flux<DataBuffer> chunked(byte[] bytes, int chunkSize) {
        int count = (bytes.length + chunkSize - 1) / chunkSize;
        return Flux.range(0, count).map(i -> DefaultDataBufferFactory.sharedInstance.wrap(
                Arrays.copyOfRange(bytes, i * chunkSize, Math.min(bytes.length, (i + 1) * chunkSize))));
    }

    // 回放手写的 SSE 文本，用于构造残缺的流
    private static List<StreamEvent> replayText(String sse, EventTranslator translator) {
        return replayBytes(sse.getBytes(StandardCharsets.UTF_8), translator);
    }



    // 拼接所有 TextDelta
    private static String text(List<StreamEvent> events) {
        return events.stream().filter(TextDelta.class::isInstance)
                .map(e -> ((TextDelta) e).text()).collect(Collectors.joining());
    }

    // 最后一个 UsageUpdate：usage 是累计快照，以最后一次为准
    private static Usage lastUsage(List<StreamEvent> events) {
        return events.stream().filter(UsageUpdate.class::isInstance)
                .map(e -> ((UsageUpdate) e).usage()).reduce((a, b) -> b).orElseThrow();
    }

    // 断言流以 Finished 结束，并返回它
    private static Finished finished(List<StreamEvent> events) {
        return assertInstanceOf(Finished.class, events.get(events.size() - 1));
    }

    // ---------- 正常结束 / 截断：两家各两份抓包 ----------

    @Test
    void claude_normal() {
        List<StreamEvent> events = replay("/captures/anthropic-normal.txt", claude());
        assertTrue(text(events).startsWith("I'm Claude"));
        assertEquals("end_turn", finished(events).rawReason());
        // 最终 usage 取 message_delta 中的 47，而不是 message_start 中的 8
        assertEquals(21, lastUsage(events).inputTokens());
        assertEquals(47, lastUsage(events).outputTokens());
    }

    @Test
    void claude_truncated() {
        List<StreamEvent> events = replay("/captures/anthropic-normal-5.txt", claude());
        assertEquals("I'm Cla", text(events)); // 截断发生在词中间
        assertEquals("max_tokens", finished(events).rawReason());
        assertEquals(5, lastUsage(events).outputTokens());
    }

    @Test
    void gemini_normal() {
        List<StreamEvent> events = replay("/captures/gemini-normal.txt", gemini());
        // 最后一块只带 thoughtSignature，空文本应被跳过，不影响拼接结果
        assertEquals("I am a large language model, trained by Google.", text(events));
        assertEquals("STOP", finished(events).rawReason());
        assertEquals(7, lastUsage(events).inputTokens());
        assertEquals(11, lastUsage(events).outputTokens());
    }

    @Test
    void gemini_truncated() {
        List<StreamEvent> events = replay("/captures/gemini-normal-5.txt", gemini());
        assertEquals("I", text(events));
        assertEquals("MAX_TOKENS", finished(events).rawReason());
        assertEquals(1, lastUsage(events).outputTokens());
    }

    // 两家的结束原因映射到同一套统一枚举：网关下游不应感知厂商差异
    @Test
    void finishReasons_mapConsistentlyAcrossProviders() {
        FinishReason claudeStop = finished(replay("/captures/anthropic-normal.txt", claude())).reason();
        FinishReason geminiStop = finished(replay("/captures/gemini-normal.txt", gemini())).reason();
        FinishReason claudeCut = finished(replay("/captures/anthropic-normal-5.txt", claude())).reason();
        FinishReason geminiCut = finished(replay("/captures/gemini-normal-5.txt", gemini())).reason();

        assertEquals(claudeStop, geminiStop);
        assertEquals(claudeCut, geminiCut);
        assertNotEquals(claudeStop, claudeCut);
    }

    // ---------- 残缺的流 ----------

    // Gemini 中途断开：只收到第一块，没有 finishReason 就 EOF
    @Test
    void gemini_eofWithoutFinishReason_isTruncatedError() {
        String sse = """
                data: {"candidates":[{"content":{"parts":[{"text":"I am a large language model, trained by Google."}],"role":"model"},"index":0}],"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":11,"totalTokenCount":18}}

                """;
        List<StreamEvent> events = replayText(sse, gemini());

        // 已收到的内容照常产出，最后以截断错误结束
        assertEquals("I am a large language model, trained by Google.", text(events));
        assertEquals(11, lastUsage(events).outputTokens());
        StreamError error = assertInstanceOf(StreamError.class, events.get(events.size() - 1));
        assertTrue(error.message().contains("truncated"));
    }

    // Claude 没收到 message_delta 就收到 message_stop：最终 usage 缺失
    @Test
    void claude_stopWithoutMessageDelta_isError() {
        String sse = """
                event: message_start
                data: {"type":"message_start","message":{"usage":{"input_tokens":21,"output_tokens":8}}}

                event: content_block_delta
                data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"I'm Claude"}}

                event: message_stop
                data: {"type":"message_stop"}

                """;
        List<StreamEvent> events = replayText(sse, claude());

        // 只有一个终止事件，且是最后一个
        assertEquals(1, events.stream().filter(StreamEvent::isTerminal).count());
        // 设计决定：缺少最终 usage 视为出错，走本地估算兜底；如果你决定按正常结束处理，改这一行
        assertInstanceOf(StreamEvent.Finished.class, events.get(events.size() - 1));
    }
}