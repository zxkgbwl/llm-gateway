package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.provider.StreamEvent.Finished;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.StreamError;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.TextDelta;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.UsageUpdate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class OpenAiTranslatorTest {

    private static final String NORMAL_STREAM = """
            data: {"choices":[{"index":0,"delta":{"role":"assistant","content":""}}],"usage":null}

            data: {"choices":[{"index":0,"delta":{"content":"Hi"}}],"usage":null}

            data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}],"usage":null}

            data: {"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":3}}

            data: [DONE]

            """;

    @Test
    void deliversUsageBeforeFinishedEvenThoughFinishReasonComesFirst() {
        List<StreamEvent> events = Recorded.replay(NORMAL_STREAM, new OpenAiAdapter("openai", "x", "k").newTranslator());

        assertEquals(List.of(
                new TextDelta("Hi"),
                new UsageUpdate(new Usage(9, 3)),
                new Finished(FinishReason.LENGTH, "length")), events);
    }

    @Test
    void streamCutBeforeDoneIsReportedAsTruncated() {
        String cut = """
                data: {"choices":[{"index":0,"delta":{"content":"Hi"}}],"usage":null}

                """;
        List<StreamEvent> events = Recorded.replay(cut, new OpenAiAdapter("openai", "x", "k").newTranslator());

        assertEquals(new TextDelta("Hi"), events.get(0));
        assertInstanceOf(StreamError.class, events.get(1));
    }
}
