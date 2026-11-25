package io.github.zxkgbwl.llmgateway.provider;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Optional;



/**
 * 提供方在任何数据流启动之前就拒绝了请求（返回了非 2xx 状态码）。
 * 由于尚未向客户端发送任何内容，因此这类失败情况可以安全地进行重试，
 * 或者将其路由至备用模型。
 */

public final class UpstreamHttpException extends IOException {

    private static final int BRIEF_MAX_CHARS = 200;
    private final String provider;
    private final int statusCode;
    private final String retryAfter;
    private final String body;

    public UpstreamHttpException(String provider, int statusCode, String retryAfter, String body) {
        // 异常消息里不放 body：body 可能很长、含内部信息，只在日志里完整记录
        super(provider + " returned HTTP " + statusCode + ": " + body);
        this.provider = provider;
        this.statusCode = statusCode;
        this.retryAfter = retryAfter;
        this.body = body;
    }

    public String provider() {
        return provider;
    }

    public int statusCode() {
        return statusCode;
    }

    /**
     * Raw value of the Retry-After header, if the provider sent one.
     */
    public String retryAfter() {

        return retryAfter;
    }

    public String body() {

        return body;
    }

    /**
     * 给客户端看的简要说明：只取上游 JSON 里的 error.message（Gemini、Anthropic、OpenAI 兼容格式都在这个位置），并截断。
     */
    public String briefMessage() {
        String detail = "";
        try {
            JsonNode message = UpstreamJson.parse(body).path("error").path("message");
            if (message.isTextual()) {
                detail = message.asText();
            }
        } catch (RuntimeException ignored) {
            // 不是 JSON 就不带细节
        }
        if (detail.length() > BRIEF_MAX_CHARS) {
            detail = detail.substring(0, BRIEF_MAX_CHARS) + "...";
        }
        String head = provider + " rejected the request (HTTP " + statusCode + ")";

        return detail.isEmpty() ? head : head + ": " + detail;
    }

    /** 429 means "slow down", not "broken": it should not count as a circuit-breaker failure. */
    public boolean isRateLimited() {
        return statusCode == 429;
    }
}
