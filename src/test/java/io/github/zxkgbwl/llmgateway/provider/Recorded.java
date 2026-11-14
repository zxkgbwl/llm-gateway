package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.sse.SseReader;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Replays a recorded SSE stream through the real SseReader, EventTranslator and ChatStream,
 * with no network. Paste raw curl output into a test to add a regression case.
 */
final class Recorded {

    private Recorded() {
    }

    static List<StreamEvent> replay(String rawSse, EventTranslator translator) {
        SseReader reader = new SseReader(new ByteArrayInputStream(rawSse.getBytes(StandardCharsets.UTF_8)));
        List<StreamEvent> events = new ArrayList<>();
        try (ChatStream stream = new ChatStream(reader, translator)) {
            stream.forEachRemaining(events::add);
        }
        return events;
    }
}
