package io.github.zxkgbwl.llmgateway.api;

import io.github.zxkgbwl.llmgateway.provider.UpstreamHttpException;
import io.github.zxkgbwl.llmgateway.provider.UpstreamUnavailableException;
import io.github.zxkgbwl.llmgateway.quota.QuotaExceededException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 流开始前的错误：响应头还没发出，仍然可以返回正常的 HTTP 状态码。
 * 返回给客户端的只有简要说明，完整的上游错误体已经在 FailoverClient 里写进日志。
 */
@RestControllerAdvice
public class GatewayExceptionHandler {

    /**
     * 经过 FailoverClient 之后，能走到这里的 UpstreamHttpException 只剩 CLIENT_ERROR（上游 400 等）：
     * 状态码原样返回，客户端知道是自己的请求有问题。
     */
    @ExceptionHandler(UpstreamHttpException.class)
    public ResponseEntity<Map<String, Object>> upstream(UpstreamHttpException e) {
        int status = (e.statusCode() >= 400 && e.statusCode() < 500) ? e.statusCode() : HttpStatus.BAD_GATEWAY.value();
        return ResponseEntity.status(status)
                .body(error(e.briefMessage(), "invalid_request_error"));
    }

    /** 所有候选都失败：全是 429 / 冷却 → 429 + Retry-After；其余 → 503 */
    @ExceptionHandler(UpstreamUnavailableException.class)
    public ResponseEntity<Map<String, Object>> unavailable(UpstreamUnavailableException e) {
        if (e.rateLimited()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", String.valueOf(e.retryAfterSeconds()))
                    .body(error(e.getMessage(), "upstream_rate_limit"));
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(error(e.getMessage(), "upstream_unavailable"));
    }

    /** 网关自己的 TPM 限流。type 和上游的 upstream_rate_limit 区分开，客户端能看出是谁限的流 */
    @ExceptionHandler(QuotaExceededException.class)
    public ResponseEntity<Map<String, Object>> quota(QuotaExceededException e) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", String.valueOf(e.retryAfterSeconds()))
                .header("x-ratelimit-remaining-tokens", String.valueOf(Math.max(0, e.remainingTokens())))
                .body(Map.of("error", Map.of(
                        "message", e.getMessage(),
                        "type", "gateway_rate_limit",
                        "code", "tpm_exceeded")));
    }

    private static Map<String, Object> error(String message, String type) {
        return Map.of("error", Map.of("message", message, "type", type));
    }
}
