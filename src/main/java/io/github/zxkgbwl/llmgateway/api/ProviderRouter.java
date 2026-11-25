package io.github.zxkgbwl.llmgateway.api;

import io.github.zxkgbwl.llmgateway.provider.Candidate;
import io.github.zxkgbwl.llmgateway.provider.ProtocolAdapter;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 按模型名的前缀路由到对应厂商。
 */
public final class ProviderRouter {

    private final Map<String, ProtocolAdapter> byPrefix;
    private final Map<String, List<Candidate>> aliases;

    public ProviderRouter(Map<String, ProtocolAdapter> byPrefix,
                          Map<String, ProtocolAdapter> byProvider,
                          Map<String, List<ModelAliasProperties.CandidateConfig>> models) {

        this.byPrefix = Map.copyOf(byPrefix);

        // 启动时就把别名解析成候选，配错了直接启动失败，不要等到请求时才发现
        Map<String, List<Candidate>> resolved = new HashMap<>();
        models.forEach((alias, configs) -> {
            if (configs == null || configs.isEmpty()) {
                throw new IllegalStateException("gateway.models." + alias + " has no candidates");
            }
            resolved.put(alias, configs.stream().map(config -> {
                ProtocolAdapter adapter = byProvider.get(config.provider());
                if (adapter == null) {
                    throw new IllegalStateException("gateway.models." + alias
                            + ": unknown provider '" + config.provider() + "', known: " + byProvider.keySet());
                }
                return new Candidate(adapter, config.model());
            }).toList());
        });

        this.aliases = Map.copyOf(resolved);
    }

        public List<Candidate> route(String model) {
            List<Candidate> alias = aliases.get(model);
            if (alias != null) {
                return alias;
            }
            return byPrefix.entrySet().stream()
                    .filter(e -> model != null && model.startsWith(e.getKey()))
                    .map(e -> List.of(new Candidate(e.getValue(), model)))
                    .findFirst()
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported model: " + model));
        }
}