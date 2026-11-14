package io.github.zxkgbwl.llmgateway.api;

import io.github.zxkgbwl.llmgateway.provider.ProtocolAdapter;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/**
 * 按模型名的前缀路由到对应厂商。第 7 周的按成本、延迟路由会替换这里的实现。
 */
public final class ProviderRouter {

    private final Map<String, ProtocolAdapter> byPrefix;

    public ProviderRouter(Map<String, ProtocolAdapter> byPrefix) {
        this.byPrefix = Map.copyOf(byPrefix);
    }

    public ProtocolAdapter route(String model) {
        return byPrefix.entrySet().stream()
                .filter(e -> model != null && model.startsWith(e.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported model: " + model));
    }
}