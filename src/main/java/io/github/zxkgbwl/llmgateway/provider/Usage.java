package io.github.zxkgbwl.llmgateway.provider;

/**
 * 一次调用的 token 用量。
 *
 * <p>outputTokens 包含思考 token（按输出计费）。inputTokens 包含缓存读取的部分，
 * cacheReadTokens 是其中的子集：Gemini / OpenAI 本来就这样报，Anthropic 在 adapter 里加回去。
 */
public record Usage(int inputTokens, int outputTokens, int cacheReadTokens) {

    /** 没有缓存信息的厂商（如 Ollama）和旧代码用 */
    public Usage(int inputTokens, int outputTokens) {
        this(inputTokens, outputTokens, 0);
    }

    public int totalTokens() {
        return inputTokens + outputTokens;
    }
}
