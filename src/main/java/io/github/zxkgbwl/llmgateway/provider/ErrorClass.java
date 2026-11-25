package io.github.zxkgbwl.llmgateway.provider;

import org.springframework.web.reactive.function.client.WebClientRequestException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 首字节前错误的分类，决定 FailoverClient 下一步怎么做。
 *
 * <p>首字节后的错误已经在 StreamingClient 里变成 StreamError 事件，不会走到这里。
 */
public enum ErrorClass {
    /** 连接失败、获取连接池超时、首字节前超时、上游 5xx / 529：同一候选重试 1 次，再切换 */
    RETRYABLE,
    /** 上游 429：记冷却，直接切换（马上重试同一个只会再吃一次 429） */
    RATE_LIMITED,
    /** 上游 401 / 403：Key 或权限配错了，重试没有意义；切换并打 ERROR 日志 */
    PROVIDER_MISCONFIGURED,
    /** 上游 400 等其余 4xx：请求本身有问题，换厂商大概率一样；不重试，返回给客户端 */
    CLIENT_ERROR,
    /** 网关自身的 bug（比如构造上游请求时抛异常）：不重试，按 500 处理 */
    INTERNAL;

    public static ErrorClass of(Throwable e) {
        // UpstreamHttpException 也是 IOException，必须先判断
        if (e instanceof UpstreamHttpException http) {
            return ofStatus(http.statusCode());
        }
        // WebClientRequestException 包住了连接拒绝、DNS 失败、获取连接超时（PoolAcquireTimeoutException）等；
        // TimeoutException 来自首字节超时；IOException 覆盖 PrematureCloseException 这类连接中断
        if (e instanceof WebClientRequestException || e instanceof TimeoutException || e instanceof IOException) {
            return RETRYABLE;
        }
        return INTERNAL;
    }




    public static ErrorClass ofStatus(int statusCode) {
        if (statusCode == 429) {
            return RATE_LIMITED;
        }

        if (statusCode == 401 || statusCode == 403) {
            return PROVIDER_MISCONFIGURED;
        }

        // 529 是 Anthropic 的 overloaded，包含在 >= 500 里
        if (statusCode >= 500 || statusCode == 408) {
            return RETRYABLE;
        }
        return CLIENT_ERROR;
    }

}
