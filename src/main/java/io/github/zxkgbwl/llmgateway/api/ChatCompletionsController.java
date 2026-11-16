package io.github.zxkgbwl.llmgateway.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.provider.ProtocolAdapter;
import io.github.zxkgbwl.llmgateway.provider.StreamingClient;
import io.github.zxkgbwl.llmgateway.quota.QuotaSettler;
import io.github.zxkgbwl.llmgateway.tenant.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
public class ChatCompletionsController {
    private static final Logger log = LoggerFactory.getLogger(ChatCompletionsController.class);

    private final StreamingClient client;
    private final ProviderRouter router;
    private final ObjectMapper json;
    private final QuotaSettler settler;
    private final double inputFactor;

    public ChatCompletionsController(StreamingClient client, ProviderRouter router,  ObjectMapper json,
                                     QuotaSettler settler,
                                     @Value("${gateway.quota.input-factor: 1.3}") double inputFactor) {
        this.client = client;
        this.router = router;
        this.json = json;
        this.settler = settler;
        this.inputFactor = inputFactor;
    }

    @PostMapping("/v1/chat/completions")
    public ResponseEntity<Flux<ServerSentEvent<String>>> chat(@RequestBody ChatCompletionRequest request,
                                                              ServerWebExchange exchange) {
        if (!Boolean.TRUE.equals(request.stream())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only stream=true is supported");
        }
        Tenant tenant = exchange.getRequiredAttribute(Tenant.ATTR);
        ProtocolAdapter adapter = router.route(request.model());
        ChatRequest chatRequest = toChatRequest(request, tenant.defaultMaxTokens());
        int estimatedInput = QuotaSettler.estimateInput(chatRequest, inputFactor);

        Flux<ServerSentEvent<String>> body = Flux.defer(() ->{
            //编码器有状态，每次新建一个
            OpenAiChunkEncoder encoder = new OpenAiChunkEncoder(json, request.model(), request.includeUsage());
            //client.stream返回Flux<StreamEvent>, 通过encode 转换为Flux<String> 然后map成Flux<ServerSentEvent<String>>（加壳）
            return settler.meter(tenant, request.model(), estimatedInput, chatRequest.maxTokens(),
                    () -> client.stream(adapter, chatRequest))
                    .concatMapIterable(encoder::encode)
                    .map(data-> ServerSentEvent.<String>builder(data).build());
        })
        .doOnCancel(()-> log.info("客户端断开，取消上游请求：model={}", request.model()))
        .doFinally(signal->log.info("流结束：model={}, signal={}", request.model(), signal));

        return ResponseEntity.ok()
                .header("X-Accel-Buffering", "no")
                .body(body);
    }

    // OpenAI 请求 → 内部统一请求;
    private static ChatRequest toChatRequest(ChatCompletionRequest request, int defaultMaxTokens) {
        int maxTokens = request.maxTokens() != null ? request.maxTokens() : defaultMaxTokens;
        List<ChatRequest.Message> messages = request.messages().stream()
                .map(m -> new ChatRequest.Message(ChatRequest.Role.valueOf(m.role().toUpperCase()), m.content())) // 如果内部的 role 是枚举，在这里做映射
                .toList();
        return new ChatRequest(request.model(), messages, maxTokens);
    }

 }
