package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.provider.StreamEvent.StreamError;
import io.github.zxkgbwl.llmgateway.sse.SseEvent;
import io.github.zxkgbwl.llmgateway.sse.SseReader;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A pull-based stream of {@link StreamEvent}s for one call.
 *
 * <p>Guarantees, regardless of provider:
 * <ul>
 *   <li>iteration always ends with exactly one terminal event (Finished or StreamError)</li>
 *   <li>mid-stream I/O errors and translation errors become a StreamError, never an exception</li>
 *   <li>a stream that ends without a finish signal is reported as truncated</li>
 *   <li>the connection is closed as soon as a terminal event is produced</li>
 * </ul>
 *
 * <p>Always use try-with-resources: closing early also aborts the upstream request,
 * which stops the provider from generating (and billing) tokens nobody will read.
 */
public final class ChatStream implements Iterator<StreamEvent>, AutoCloseable {

    private final SseReader reader;
    private final EventTranslator translator;
    private final Deque<StreamEvent> pending = new ArrayDeque<>();
    private boolean terminated;

    public ChatStream(SseReader reader, EventTranslator translator) {
        this.reader = reader;
        this.translator = translator;
    }

    @Override
    public boolean hasNext() {
        while (pending.isEmpty() && !terminated) {
            pullFromUpstream();
        }
        return !pending.isEmpty();
    }

    @Override
    public StreamEvent next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return pending.poll();
    }

    private void pullFromUpstream() {
        List<StreamEvent> events;
        try {
            SseEvent sse = reader.next();
            events = (sse == null)
                    ? List.of(new StreamError("Stream ended without a finish signal; response is truncated"))
                    : translator.translate(sse);
        } catch (IOException e) {
            events = List.of(new StreamError("Connection lost mid-stream: " + e.getMessage()));
        } catch (RuntimeException e) {
            events = List.of(new StreamError("Cannot translate stream event: " + e.getMessage()));
        }

        for (StreamEvent event : events) {
            pending.add(event);
            if (event.isTerminal()) {
                terminated = true;
                close();
                return; // anything after a terminal event is ignored
            }
        }
    }

    @Override
    public void close() {
        terminated = true;
        try {
            reader.close();
        } catch (IOException ignored) {
            // closing is best effort; the stream is finished either way
        }
    }
}
