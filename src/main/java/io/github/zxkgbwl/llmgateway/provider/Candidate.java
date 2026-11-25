package io.github.zxkgbwl.llmgateway.provider;


/**
 * 模型别名展开后的一个候选：哪个厂商 + 发给它的真实模型名。
 */
public record Candidate(ProtocolAdapter adapter, String model) {

    public String key() {
        return adapter.providerName() + ":" + model;
    }
}


