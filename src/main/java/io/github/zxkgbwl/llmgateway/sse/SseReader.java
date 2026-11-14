package io.github.zxkgbwl.llmgateway.sse;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Pull-based SSE reader: call {@link #next()} until it returns {@code null}.
 *
 * <p>Implements the parts of the SSE spec that LLM APIs use:
 * <ul>
 *   <li>a blank line terminates an event</li>
 *   <li>"event:" sets the event name; "data:" may repeat and is joined with '\n'</li>
 *   <li>lines starting with ':' are comments (often used as heartbeats) and are ignored</li>
 *   <li>a single space after the colon is stripped</li>
 * </ul>
 */
public final class SseReader implements Closeable {

    private final BufferedReader reader;
    private final Consumer<String> rawLineListener;

    public SseReader(InputStream in) {
        this(in, line -> { });
    }

    /**
     * @param rawLineListener receives every raw line before parsing; useful for debugging
     */
    public SseReader(InputStream in, Consumer<String> rawLineListener) {
        this.reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        this.rawLineListener = rawLineListener;
    }

    /**
     * Blocks until the next complete event arrives.
     *
     * @return the next event, or {@code null} at end of stream
     */
    public SseEvent next() throws IOException {
        String eventName = null;
        StringBuilder data = null;

        String line;
        while ((line = reader.readLine()) != null) {
            rawLineListener.accept(line);

            if (line.isEmpty()) {
                if (data != null) {
                    return new SseEvent(eventName, data.toString());
                }
                eventName = null; // blank line with no data: nothing to dispatch
                continue;
            }
            if (line.startsWith(":")) {
                continue;
            }

            int colon = line.indexOf(':');
            String field = colon < 0 ? line : line.substring(0, colon);
            String value = colon < 0 ? "" : line.substring(colon + 1);
            if (value.startsWith(" ")) {
                value = value.substring(1);
            }

            switch (field) {
                case "event" -> eventName = value;
                case "data" -> data = (data == null) ? new StringBuilder(value) : data.append('\n').append(value);
                default -> { /* "id" and "retry" are not used by LLM APIs */ }
            }
        }

        // The stream may end without a trailing blank line.
        return data != null ? new SseEvent(eventName, data.toString()) : null;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
