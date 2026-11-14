package io.github.zxkgbwl.llmgateway.api;

// import 按实际包路径调整

import io.github.zxkgbwl.llmgateway.provider.UpstreamHttpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 流开始前的上游错误：响应头还没发出，仍然可以返回正常的 HTTP 状态码。
 */
@RestControllerAdvice
public class GatewayExceptionHandler {

    @ExceptionHandler(UpstreamHttpException.class)
    public ResponseEntity<Map<String, Object>> upstream(UpstreamHttpException e) {
        // 429 原样转给客户端，并带上 retry-after；其他上游错误统一返回 502，表示问题出在上游，不在客户端
        // getter 名称按你的 UpstreamHttpException 调整
        boolean rateLimited = e.statusCode() == 429;
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(
                rateLimited ? HttpStatus.TOO_MANY_REQUESTS : HttpStatus.BAD_GATEWAY);

        if (rateLimited && e.retryAfter() != null) {
            builder.header("Retry-After", e.retryAfter());
        }
        return builder.body(Map.of("error", Map.of(
                "message", e.getMessage(),
                "type", "upstream_error")));
    }
}