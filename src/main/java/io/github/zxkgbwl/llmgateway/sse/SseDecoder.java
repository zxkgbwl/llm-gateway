package io.github.zxkgbwl.llmgateway.sse;

import reactor.core.publisher.Flux;


import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;

import java.util.function.Consumer;


/**
 * 推模式 SSE 解码：Flux<DataBuffer> → Flux<SseEvent>。
 * 数据块的边界是任意的，可能把一行、甚至一个汉字切开，所以先在字节层面缓冲，切出完整的行之后再解码。
 */
public class SseDecoder {
    private final Consumer<String> rawLIneListener;

    // 调试用的原始行监听器在构造时注入
    public SseDecoder(Consumer<String> rawLIneListener) {
        this.rawLIneListener = rawLIneListener;
    }

    public Flux<SseEvent> decode(Flux<DataBuffer> body) {
        return Flux.defer(()->{
            State state = new State();
            return body.concatMapIterable(buffer-> state.feed(drain(buffer)))
                    .concatWith(Flux.defer(()-> Flux.fromIterable(state.finish())));
        });
    }

    // 拷贝出字节后立即释放 DataBuffer：底层是 Netty 的池化内存，不释放会泄漏
    private static byte[] drain(DataBuffer buffer) {
        try {
            byte[] bytes = new byte[buffer.readableByteCount()];
            buffer.read(bytes);
            return bytes;

        }finally {
            DataBufferUtils.release(buffer);
        }
    }

    private final class State {

        private final ByteArrayOutputStream pendingLine =
                new ByteArrayOutputStream();

        private String eventName;
        private StringBuilder data;

        // 上一个字节是否为 CR，等待判断是否组成 CRLF
        private boolean pendingCR;

        List<SseEvent> feed(byte[] bytes) {
            List<SseEvent> out = new ArrayList<>();

            for (byte b : bytes) {

                // 上一个字节是 CR
                if (pendingCR) {
                    pendingCR = false;

                    // CRLF：两个字节组成一个换行符
                    if (b == '\n') {
                        handleLine(takeLine(), out);
                        continue;
                    }

                    // 单独 CR：CR 本身就是换行符
                    handleLine(takeLine(), out);
                }

                if (b == '\r') {
                    // 暂存 CR，等待下一个字节判断
                    pendingCR = true;

                } else if (b == '\n') {
                    // 单独 LF
                    handleLine(takeLine(), out);

                } else {
                    // 普通字节，继续缓存
                    pendingLine.write(b);
                }
            }

            return out;
        }

        // 流结束时调用
        List<SseEvent> finish() {
            if (pendingLine.size() > 0) {
                // 停在半行：说明流被截断了，不能把半行当成完整的数据交出去
                throw new IllegalStateException("Stream ended in the middle of a line");
            }
            List<SseEvent> out = new ArrayList<>();
            dispatch(out); // 只有这种情况才宽容处理：最后一个事件的行都是完整的，只是缺少结尾的空行
            return out;
        }

        private String takeLine() {
            String line = pendingLine.toString(StandardCharsets.UTF_8);
            pendingLine.reset();
            return line;
        }

        private void handleLine(String line, List<SseEvent> out) {
            rawLIneListener.accept(line);

            // 空行表示一个 SSE 事件结束
            if (line.isEmpty()) {
                dispatch(out);
                return;
            }

            // 注释行
            if (line.startsWith(":")) {
                return;
            }

            int colon = line.indexOf(':');

            String field = colon < 0
                    ? line
                    : line.substring(0, colon);

            String value = colon < 0
                    ? ""
                    : line.substring(colon + 1);

            // 去掉冒号后的第一个空格
            if (value.startsWith(" ")) {
                value = value.substring(1);
            }

            switch (field) {
                case "event" -> eventName = value;

                case "data" -> {
                    if (data == null) {
                        data = new StringBuilder();
                    } else {
                        data.append('\n');
                    }

                    data.append(value);
                }

                default -> {
                }
            }
        }

        private void dispatch(List<SseEvent> out) {
            if (data != null) {
                out.add(new SseEvent(eventName, data.toString()));
            }

            eventName = null;
            data = null;
        }
    }



}
