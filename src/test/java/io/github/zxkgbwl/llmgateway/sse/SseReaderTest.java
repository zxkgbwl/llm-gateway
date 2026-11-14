package io.github.zxkgbwl.llmgateway.sse;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SseReaderTest {

    // 把拉模式的 next() 循环调用到返回 null 为止，转换成"输入字符串、输出列表"，每个用例只需两三行
    private static List<SseEvent> parse(String raw) throws IOException {
        List<SseEvent> events = new ArrayList<>();
        try (SseReader reader = new SseReader(toStream(raw))) {
            SseEvent event;
            while ((event = reader.next()) != null) {
                events.add(event);
            }
        }
        return events;
    }

    private static ByteArrayInputStream toStream(String raw) {
        return new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8));
    }

    // ===== 以下 7 个用例从 SseParserTest 迁移而来，断言内容不变 =====

    @Test
    void parsesNamedEvent() throws IOException {
        List<SseEvent> events = parse("event: message_start\ndata: {\"a\":1}\n\n");
        assertEquals(1, events.size());
        assertEquals("message_start", events.get(0).event());
        assertEquals("{\"a\":1}", events.get(0).data());
    }

    // 以冒号开头的注释行（心跳）被忽略
    @Test
    void ignoreTheLineStartsWithColon() throws IOException {
        List<SseEvent> events = parse(": ping\nevent: message_start\ndata: {\"a\":1}\n\n");
        assertEquals(1, events.size());
        assertEquals("message_start", events.get(0).event());
        assertEquals("{\"a\":1}", events.get(0).data());
    }

    // 没有 event 行时，event() 为 null（OpenAI 和 Gemini 的情况）
    @Test
    void eventIsNullIfNoEventLine() throws IOException {
        List<SseEvent> events = parse("data: {\"a\":1}\n\n");
        assertEquals(1, events.size());
        assertNull(events.get(0).event());
        assertEquals("{\"a\":1}", events.get(0).data());
    }

    // 多行 data 用 \n 拼接成一个事件
    @Test
    void combineMultipleLineOfDataInAEvent() throws IOException {
        List<SseEvent> events = parse("event: message_start\ndata: {\"a\":1}\ndata: {\"b\":2}\n\n");
        assertEquals(1, events.size());
        assertEquals("message_start", events.get(0).event());
        assertEquals("{\"a\":1}\n{\"b\":2}", events.get(0).data());
    }

    // 最后一个事件没有以空行结尾，也能被解析出来
    @Test
    void lastEventCouldBeParsedWithOutBlankLine() throws IOException {
        List<SseEvent> events = parse("event: message_start\ndata: {\"a\":1}\n\n"
                + "event: message_end\ndata: {\"b\":2}");
        assertEquals(2, events.size());
        assertEquals("message_start", events.get(0).event());
        assertEquals("{\"a\":1}", events.get(0).data());
        assertEquals("message_end", events.get(1).event());
        assertEquals("{\"b\":2}", events.get(1).data());
    }

    // "data:" 后面没有空格时，值照样正确
    @Test
    void dataCouldBeParsedWithoutBlankAfterColon() throws IOException {
        List<SseEvent> events = parse("event: message_start\ndata:{\"a\":1}\n\n");
        assertEquals(1, events.size());
        assertEquals("message_start", events.get(0).event());
        assertEquals("{\"a\":1}", events.get(0).data());
    }

    // 连续多个空行不会产生空事件
    @Test
    void multiBlankLineNotGenerateEmptyEvents() throws IOException {
        List<SseEvent> events = parse("event: message_start\ndata: {\"a\":1}\n\n\n\n\n"
                + "event: message_end\ndata: {\"b\":2}");
        assertEquals(2, events.size());
        assertEquals("message_start", events.get(0).event());
        assertEquals("{\"a\":1}", events.get(0).data());
        assertEquals("message_end", events.get(1).event());
        assertEquals("{\"b\":2}", events.get(1).data());
    }

    // ===== 以下为新增用例 =====

    // 拉模式特有：读到流尾后返回 null，再调用仍然返回 null，不会抛异常
    @Test
    void nextKeepsReturningNullAfterEndOfStream() throws IOException {
        try (SseReader reader = new SseReader(toStream("data: 1\n\n"))) {
            assertEquals("1", reader.next().data());
            assertNull(reader.next());
            assertNull(reader.next());
        }
    }

    // 空输入不产生任何事件
    @Test
    void emptyInputProducesNoEvents() throws IOException {
        assertEquals(0, parse("").size());
    }

    // 事件名只属于当前事件，不会"泄漏"到下一个没有 event 行的事件
    @Test
    void eventNameDoesNotLeakIntoNextEvent() throws IOException {
        List<SseEvent> events = parse("event: message_start\ndata: 1\n\ndata: 2\n\n");
        assertEquals(2, events.size());
        assertEquals("message_start", events.get(0).event());
        assertNull(events.get(1).event());
    }

    // 只有 event 行、没有 data 的块不产生事件，且它的事件名会被丢弃
    @Test
    void eventLineWithoutDataIsDiscarded() throws IOException {
        List<SseEvent> events = parse("event: orphan\n\ndata: 1\n\n");
        assertEquals(1, events.size());
        assertNull(events.get(0).event());
        assertEquals("1", events.get(0).data());
    }

    // 有些服务端用 CRLF 换行，解析结果应与 LF 一致
    @Test
    void handlesCrlfLineEndings() throws IOException {
        List<SseEvent> events = parse("event: message_start\r\ndata: {\"a\":1}\r\n\r\n");
        assertEquals(1, events.size());
        assertEquals("message_start", events.get(0).event());
        assertEquals("{\"a\":1}", events.get(0).data());
    }

    // 中文等多字节字符按 UTF-8 正确解码，不出现乱码
    @Test
    void decodesUtf8Text() throws IOException {
        List<SseEvent> events = parse("data: {\"text\":\"你好，世界\"}\n\n");
        assertEquals("{\"text\":\"你好，世界\"}", events.get(0).data());
    }

    // rawLineListener 收到每一行原始文本，包括注释行和空行，用于协议调试
    @Test
    void rawLineListenerSeesEveryLine() throws IOException {
        List<String> rawLines = new ArrayList<>();
        try (SseReader reader = new SseReader(toStream(": ping\ndata: 1\n\n"), rawLines::add)) {
            while (reader.next() != null) {
                // 只为触发读取
            }
        }
        assertEquals(List.of(": ping", "data: 1", ""), rawLines);
    }
}
