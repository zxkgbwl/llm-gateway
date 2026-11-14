package io.github.zxkgbwl.llmgateway.provider;

/**
 * Token usage of one call.
 *
 * <p>{@code outputTokens} includes hidden reasoning/thinking tokens when a provider reports
 * them separately, because they are billed as output.
 */
public record Usage(int inputTokens, int outputTokens) {

    public int totalTokens() {
        return inputTokens + outputTokens;
    }
}
