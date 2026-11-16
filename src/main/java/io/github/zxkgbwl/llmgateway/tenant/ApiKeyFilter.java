package io.github.zxkgbwl.llmgateway.tenant;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class ApiKeyFilter implements WebFilter {
    private static final byte[] UNAUTHORIZED = """
            {"error":{"message":"Invalid API key","type":"invalid_request_error","code":"invalid_api_key"}}"""
            .getBytes(StandardCharsets.UTF_8);

    private final TenantRegistry registry;

    public ApiKeyFilter(TenantRegistry registry) {
        this.registry = registry;
    }



    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!exchange.getRequest().getPath().value().startsWith("/v1")) {
            return chain.filter(exchange);
        }

        String auth = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        Optional<Tenant> tenant = (auth != null && auth.startsWith("Bearer ")) ? registry.findByKey(auth.substring("Bearer ".length())) : Optional.empty();

        if (tenant.isEmpty()) {
            ServerHttpResponse response = exchange.getResponse();
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
            response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
            return response.writeWith(Mono.just(response.bufferFactory().wrap(UNAUTHORIZED)));
        }
        exchange.getAttributes().put(Tenant.ATTR, tenant.get());
        return chain.filter(exchange);
    }
}
