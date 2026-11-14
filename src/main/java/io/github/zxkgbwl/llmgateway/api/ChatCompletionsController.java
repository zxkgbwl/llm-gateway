package io.github.zxkgbwl.llmgateway.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import io.github.zxkgbwl.llmgateway.provider.ProtocolAdapter;
import io.github.zxkgbwl.llmgateway.provider.StreamingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
public class ChatCompletionsController {
    private static final Logger log = LoggerFactory.getLogger(ChatCompletionsController.class);

    private final StreamingClient client;
    private final ProviderRouter router;
    private final ObjectMapper json;

    public ChatCompletionsController(StreamingClient client, ProviderRouter router,  ObjectMapper json) {
        this.client = client;
        this.router = router;
        this.json = json;
    }

    @PostMapping("/v1/chat/completions")
    public ResponseEntity<Flux<ServerSentEvent<String>>> chat(@RequestBody ChatCompletionRequest request) {
        if (!Boolean.TRUE.equals(request.stream())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only stream=true is supported");
        }

        ProtocolAdapter adapter = router.route(request.model());
        ChatRequest chatRequest = toChatRequest(request);

        Flux<ServerSentEvent<String>> body = Flux.defer(() ->{
            //编码器有状态，每次新建一个
            OpenAiChunkEncoder encoder = new OpenAiChunkEncoder(json, request.model(), request.includeUsage());
            //client.stream返回Flux<StreamEvent>, 通过encode 转换为Flux<String> 然后map成Flux<ServerSentEvent<String>>（加壳）
            return client.stream(adapter, chatRequest)
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
    private static ChatRequest toChatRequest(ChatCompletionRequest request) {
        int maxTokens = request.maxTokens() != null ? request.maxTokens() : 1024;
        List<ChatRequest.Message> messages = request.messages().stream()
                .map(m -> new ChatRequest.Message(ChatRequest.Role.valueOf(m.role().toUpperCase()), m.content())) // 如果内部的 role 是枚举，在这里做映射
                .toList();
        return new ChatRequest(request.model(), messages, maxTokens);
    }

 }
