package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.provider.StreamEvent.Finished;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.TextDelta;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.UsageUpdate;
import io.github.zxkgbwl.llmgateway.sse.SseReader;
import org.junit.jupiter.api.Test;


import io.github.zxkgbwl.llmgateway.provider.StreamEvent.StreamError;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GeminiTranslatorTest {

    // 把一段 SSE 文本包装成 ChatStream，并收集产出的全部事件
    private static List<StreamEvent> run(String sse, EventTranslator translator) {
        InputStream in = new ByteArrayInputStream(sse.getBytes(StandardCharsets.UTF_8));
        return collect(new ChatStream(new SseReader(in, line -> {}), translator));
    }

    private static List<StreamEvent> collect(ChatStream stream) {
        List<StreamEvent> events = new ArrayList<>();
        stream.forEachRemaining(events::add);
        return events;
    }

    // Replace with your own curl capture from step 2.
    private static final String NORMAL_STREAM = """
            data: {"candidates":[{"content":{"parts":[{"text":"Hel"}],"role":"model"}}],"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":1}}

            data: {"candidates":[{"content":{"parts":[{"text":"lo"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":2,"thoughtsTokenCount":4}}

            """;

    @Test
    void usageIsCumulativeAndIncludesThinkingTokens() {
        List<StreamEvent> events = Recorded.replay(NORMAL_STREAM, new GeminiAdapter("x", "k").newTranslator());

        assertEquals(List.of(
                new TextDelta("Hel"),
                new UsageUpdate(new Usage(8, 1)),
                new TextDelta("lo"),
                new UsageUpdate(new Usage(8, 6)),
                new Finished(FinishReason.STOP, "STOP")), events);
    }

    @Test
    void blockedPromptFinishesWithContentFilter() {
        String blocked = """
                data: {"promptFeedback":{"blockReason":"SAFETY"}}

                """;
        List<StreamEvent> events = Recorded.replay(blocked, new GeminiAdapter("x", "k").newTranslator());

        assertEquals(List.of(new Finished(FinishReason.CONTENT_FILTER, "PROMPT_BLOCKED:SAFETY")), events);
    }

    @Test
    void gemini_streamEndsWithoutFinishReason() {
        // 取自 gemini-normal.txt 的第一块：有文本和 usage，但没有 finishReason
        String sse = """
            data: {"candidates":[{"content":{"parts":[{"text":"I am a large language model, trained by Google."}],"role":"model"},"index":0}],"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":11,"totalTokenCount":18}}

            """;
        GeminiAdapter adapter = new GeminiAdapter("x", "k");
        List<StreamEvent> events = run(sse, adapter.newTranslator());

        // 已收到的内容照常产出，最后以截断错误结束
        assertInstanceOf(TextDelta.class, events.get(0));
        assertInstanceOf(UsageUpdate.class, events.get(1));
        StreamError error = assertInstanceOf(StreamError.class, events.get(2));
        assertTrue(error.message().contains("truncated"));
        assertEquals(3, events.size());
    }
}
