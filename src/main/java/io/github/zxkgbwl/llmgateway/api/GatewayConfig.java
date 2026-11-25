package io.github.zxkgbwl.llmgateway.api;

// import 按实际包路径调整

import io.github.zxkgbwl.llmgateway.provider.*;
import io.github.zxkgbwl.llmgateway.quota.QuotaSettler;
import io.github.zxkgbwl.llmgateway.quota.RedisTpmLimiter;
import io.github.zxkgbwl.llmgateway.quota.TpmLimiter;
import io.github.zxkgbwl.llmgateway.sse.SseDecoder;
import io.github.zxkgbwl.llmgateway.tenant.ApiKeyFilter;
import io.netty.channel.ChannelOption;
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

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 组件装配：所有依赖在构造时注入。API Key 从环境变量读取。
 */
@Configuration
@EnableConfigurationProperties({TenantProperties.class, ModelAliasProperties.class})
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
    public StreamingClient streamingClient(WebClient.Builder builder,
                                           @Value("${gateway.timeouts.connect-ms:3000}") int connectMs,
                                           @Value("${gateway.timeouts.idle-seconds:30}") long idleSeconds) {
        // 每个流式请求会独占一条上游连接直到结束，连接池的大小决定了网关能同时承载多少个流
        ConnectionProvider pool = ConnectionProvider.builder("upstream")
                .maxConnections(2000)
                .pendingAcquireMaxCount(-1)
                .build();
        WebClient webClient = builder
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create(pool)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,connectMs)))
                .build();
        return new StreamingClient(webClient, new SseDecoder(line -> {}), Duration.ofSeconds(idleSeconds));
    }

    @Bean
    public CooldownRegistry cooldownRegistry(
            @Value("${gateway.failover.default-cooldown-seconds:30}") long defaultCooldownSeconds) {
        return new CooldownRegistry(Clock.systemUTC(), Duration.ofSeconds(defaultCooldownSeconds));
    }

    @Bean
    public FailoverClient failoverClient(StreamingClient streamingClient, CooldownRegistry cooldowns,
                                         @Value("${gateway.failover.first-byte-budget-seconds:15}") long firstByteBudgetSeconds) {
        return new FailoverClient(streamingClient::stream, cooldowns,
                Duration.ofSeconds(firstByteBudgetSeconds), Clock.systemUTC());
    }


    @Bean
    public ProviderRouter providerRouter(@Value("${GEMINI_API_KEY}") String geminiKey,
                                         @Value("${ANTHROPIC_API_KEY}") String anthropicKey,
                                         @Value("${gateway.ollama.base-url:http://localhost:11434/v1}") String ollamaUrl,
                                         ModelAliasProperties modelProperties,
                                         Environment env) {
        ProtocolAdapter gemini = new GeminiAdapter("https://generativelanguage.googleapis.com", geminiKey);
        ProtocolAdapter anthropic = new AnthropicAdapter("https://api.anthropic.com", anthropicKey);
        // Ollama 提供 OpenAI 兼容接口，直接复用 OpenAiAdapter；本地服务不校验 Key，随便填一个
        ProtocolAdapter ollama = new OpenAiAdapter("ollama", ollamaUrl, "ollama");

        Map<String, ProtocolAdapter> byPrefix = new HashMap<>();
        byPrefix.put("gemini-", gemini);
        byPrefix.put("claude-", anthropic);

        // 仅压测用：只有在 loadtest profile 下才注册假上游的路由，正常运行时 fake- 模型会返回 400
        if (env.acceptsProfiles(Profiles.of("loadtest"))) {
            byPrefix.put("fake-", new GeminiAdapter("http://localhost:8080", "fake-key"));
        }

        // 别名配置里的 provider 名字 → 厂商（第 4 步加 Ollama 时在这里加一项）
        Map<String, ProtocolAdapter> byProvider = List.of(gemini, anthropic, ollama).stream()
                .collect(Collectors.toMap(ProtocolAdapter::providerName, Function.identity()));

        return new ProviderRouter(byPrefix, byProvider, modelProperties.models());
    }
}