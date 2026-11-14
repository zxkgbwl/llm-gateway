package io.github.zxkgbwl.llmgateway.playground;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.provider.*;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.Finished;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.StreamError;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.TextDelta;
import io.github.zxkgbwl.llmgateway.provider.StreamEvent.UsageUpdate;
import io.github.zxkgbwl.llmgateway.sse.SseDecoder;
import io.github.zxkgbwl.llmgateway.token.TokenEstimator;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Week-1 experiment: stream one prompt through several providers and compare
 * TTFT, total latency, output speed and token usage.
 *
 * <pre>
 * StreamDemo [--raw] [--providers=anthropic,gemini,openai] [--prompt=...] [--max-tokens=300]
 * </pre>
 *
 * Keys and models come from environment variables; providers without a key are skipped:
 * ANTHROPIC_API_KEY / ANTHROPIC_MODEL / ANTHROPIC_BASE_URL,
 * GEMINI_API_KEY / GEMINI_MODEL / GEMINI_BASE_URL,
 * OPENAI_API_KEY / OPENAI_MODEL / OPENAI_BASE_URL.
 * Default model names go stale quickly; check each provider's docs.
 */
public final class StreamDemo {

    private static final String DEFAULT_PROMPT =
            "Explain in three sentences what an API gateway is and how an LLM gateway differs from it.";

    /** An adapter plus the model to call through it. */
    private record Target(ProtocolAdapter adapter, String model) {
    }

    /** Measurements of one call. */
    private record Result(String provider, String model, Usage usage, double ttftMs, double totalMs,
                          double tokensPerSecond, String outcome) {
    }

    private static final class RunResult {
        long firstTokenAt = -1;
        Usage usage;
        String outcome = "no terminal event";
        final StringBuilder text = new StringBuilder();
    }

    public static void main(String[] args) {
        boolean raw = false;
        String prompt = DEFAULT_PROMPT;
        int maxTokens = 300;
        List<String> wanted = List.of("anthropic", "gemini", "openai");

        for (String arg : args) {
            if (arg.equals("--raw")) {
                raw = true;
            } else if (arg.startsWith("--prompt=")) {
                prompt = arg.substring("--prompt=".length());
            } else if (arg.startsWith("--max-tokens=")) {
                maxTokens = Integer.parseInt(arg.substring("--max-tokens=".length()));
            } else if (arg.startsWith("--providers=")) {
                wanted = List.of(arg.substring("--providers=".length()).split(","));
            }
        }

        TokenEstimator.warmUp();
        StreamingClient client = new StreamingClient(WebClient.create(), new SseDecoder(line -> {}));

        Map<String, Target> configured = configuredTargets();
        List<Target> targets = new ArrayList<>();
        for (String name : wanted) {
            Target target = configured.get(name.trim());
            if (target == null) {
                System.out.printf("[skip] %s: no API key configured%n", name);
            } else {
                targets.add(target);
            }
        }
        if (targets.isEmpty()) {
            System.out.println("No provider configured. Set at least one *_API_KEY environment variable.");
            return;
        }

        System.out.printf("Prompt: %s%n", prompt);
        System.out.printf("Estimated input tokens (text only, no message overhead): o200k=%d, cl100k=%d%n",
                TokenEstimator.o200k(prompt), TokenEstimator.cl100k(prompt));

        List<Result> results = new ArrayList<>();
        for (Target target : targets) {
            results.add(run(client, target, prompt, maxTokens, raw));
        }
        printSummary(results);
    }

    private static Result run(StreamingClient client, Target target, String prompt, int maxTokens, boolean raw) {
        String provider = target.adapter().providerName();
        System.out.printf("%n======== %s / %s ========%n", provider, target.model());

        ChatRequest request = ChatRequest.ofPrompt(target.model(), prompt, maxTokens);
        long start = System.nanoTime();


        RunResult result = new RunResult();
        client.stream(target.adapter(), request)
                .doOnNext(event -> {
                    if (event instanceof TextDelta delta) {
                        if (result.firstTokenAt < 0) {
                            result.firstTokenAt = System.nanoTime();
                        }
                        result.text.append(delta.text());
                        if (!raw) {
                            System.out.print(delta.text());
                            System.out.flush();
                        }
                    } else if (event instanceof UsageUpdate update) {
                        result.usage = update.usage();
                        if (raw) {
                            System.out.printf("    <usage> %s%n", result.usage);
                        }
                    } else if (event instanceof Finished finished) {
                        result.outcome = finished.reason() + " (" + finished.rawReason() + ")";
                    } else if (event instanceof StreamError error) {
                        result.outcome = "ERROR: " + error.message();
                    }
                })
                .blockLast();//subcriber the stream and block the thread


        long end = System.nanoTime();
        double ttftMs = result.firstTokenAt < 0 ? -1 : (result.firstTokenAt - start) / 1e6;
        // Output speed covers only the decode phase, i.e. after the first token.
        double decodeSeconds = result.firstTokenAt < 0 ? 0 : (end - result.firstTokenAt) / 1e9;
        double tokensPerSecond = (result.usage != null && decodeSeconds > 0) ? result.usage.outputTokens() / decodeSeconds : 0;

        System.out.printf("%n--- outcome=%s, usage=%s%n", result.outcome, result.usage == null ? "not reported" : result.usage);
        System.out.printf("--- o200k estimate of the output text: %d tokens%n", TokenEstimator.o200k(result.text.toString()));

        return new Result(provider, target.model(), result.usage, ttftMs, (end - start) / 1e6, tokensPerSecond, result.outcome);
    }

    private static void printSummary(List<Result> results) {
        System.out.printf("%n================ Summary ================%n");
        System.out.printf("%-10s %-26s %7s %7s %9s %9s %7s  %s%n",
                "provider", "model", "in", "out", "TTFT ms", "total ms", "tok/s", "outcome");
        for (Result r : results) {
            System.out.printf("%-10s %-26s %7d %7d %9.0f %9.0f %7.1f  %s%n",
                    r.provider(), r.model(),
                    r.usage() == null ? -1 : r.usage().inputTokens(),
                    r.usage() == null ? -1 : r.usage().outputTokens(),
                    r.ttftMs(), r.totalMs(), r.tokensPerSecond(), r.outcome());
        }
    }

    private static Map<String, Target> configuredTargets() {
        Map<String, Target> targets = new LinkedHashMap<>();
        String key;
        if ((key = env("ANTHROPIC_API_KEY", null)) != null) {
            targets.put("anthropic", new Target(
                    new AnthropicAdapter(env("ANTHROPIC_BASE_URL", "https://api.anthropic.com"), key),
                    env("ANTHROPIC_MODEL", "claude-haiku-5-5")));
        }
        if ((key = env("GEMINI_API_KEY", null)) != null) {
            targets.put("gemini", new Target(
                    new GeminiAdapter(env("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com"), key),
                    env("GEMINI_MODEL", "gemini-3.1-flash-lite")));
        }
        if ((key = env("OPENAI_API_KEY", null)) != null) {
            targets.put("openai", new Target(
                    new OpenAiAdapter("openai", env("OPENAI_BASE_URL", "https://api.openai.com/v1"), key),
                    env("OPENAI_MODEL", "gpt-4o-mini")));
        }
        return targets;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
