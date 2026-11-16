package io.github.zxkgbwl.llmgateway.api;

// import 按实际包路径调整

import io.github.zxkgbwl.llmgateway.provider.AnthropicAdapter;
import io.github.zxkgbwl.llmgateway.provider.GeminiAdapter;
import io.github.zxkgbwl.llmgateway.provider.ProtocolAdapter;
import io.github.zxkgbwl.llmgateway.provider.StreamingClient;
import io.github.zxkgbwl.llmgateway.quota.QuotaSettler;
import io.github.zxkgbwl.llmgateway.quota.RedisTpmLimiter;
import io.github.zxkgbwl.llmgateway.quota.TpmLimiter;
import io.github.zxkgbwl.llmgateway.sse.SseDecoder;
import io.github.zxkgbwl.llmgateway.tenant.ApiKeyFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import io.github.zxkgbwl.llmgateway.tenant.TenantProperties;
import io.github.zxkgbwl.llmgateway.tenant.TenantRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * 组件装配：所有依赖在构造时注入。API Key 从环境变量读取。
 */
@Configuration
@EnableConfigurationProperties(TenantProperties.class)
public class GatewayConfig {
    @Bean
    public TenantRegistry tenantRegistry(TenantProperties tenantProperties) {
        return new TenantRegistry(tenantProperties.tenants());
    }

    @Bean
    public ApiKeyFilter apiKeyFilter(TenantRegistry registry) {
        return new ApiKeyFilter(registry);
    }

    @Bean
    public TpmLimiter tpmLimiter(ReactiveStringRedisTemplate redis) {
        return new RedisTpmLimiter(redis);
    }

    @Bean
    public QuotaSettler quotaSettler(TpmLimiter limiter) {
        return new QuotaSettler(limiter);
    }



    @Bean
    public StreamingClient streamingClient(WebClient.Builder builder) {
        // 每个流式请求会独占一条上游连接直到结束，连接池的大小决定了网关能同时承载多少个流
        ConnectionProvider pool = ConnectionProvider.builder("upstream")
                .maxConnections(2000)
                .pendingAcquireMaxCount(-1)
                .build();
        WebClient webClient = builder
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create(pool)))
                .build();
        return new StreamingClient(webClient, new SseDecoder(line -> {}));
    }

    @Bean
    public ProviderRouter providerRouter(@Value("${GEMINI_API_KEY}") String geminiKey,
                                         @Value("${ANTHROPIC_API_KEY}") String anthropicKey,
                                         Environment env) {
        Map<String, ProtocolAdapter> routes = new HashMap<>();
        routes.put("gemini-", new GeminiAdapter("https://generativelanguage.googleapis.com", geminiKey));
        routes.put("claude-", new AnthropicAdapter("https://api.anthropic.com", anthropicKey));

        // 仅压测用：只有在 loadtest profile 下才注册假上游的路由，正常运行时 fake- 模型会返回 400
        if (env.acceptsProfiles(Profiles.of("loadtest"))) {
            routes.put("fake-", new GeminiAdapter("http://localhost:8080", "fake-key"));
        }
        return new ProviderRouter(routes);
    }
}