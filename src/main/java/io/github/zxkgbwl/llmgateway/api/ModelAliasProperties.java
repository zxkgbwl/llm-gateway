package io.github.zxkgbwl.llmgateway.api;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

/**
 * gateway.models：模型别名 → 有序候选列表。按照优先级排序。
 */
@ConfigurationProperties(prefix = "gateway")
public record ModelAliasProperties(Map<String, List<CandidateConfig>> models) {

    public record CandidateConfig (String provider, String model) {}

    public ModelAliasProperties {
        models = models == null ? Map.of() : Map.copyOf(models);
    }
}

