package io.github.zxkgbwl.llmgateway.provider;

import io.github.zxkgbwl.llmgateway.common.ChatRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 在有序候选列表上做“首字节前重试 / 切换”。
 *
 * <p>为什么用 onErrorResume 就够了：首字节后的错误在 StreamingClient 里已经变成 StreamError 事件，
 * 能以 onError 走到这里的，一定还没收到上游首字节，此时客户端什么都还没看到，换一个上游是安全的。
 *
 * <p>首字节前有一个总预算（默认 15 秒），所有重试和切换共用；每次尝试的首字节超时 = 剩余预算。
 */

public class FailoverClient {
    private static final Logger log = LoggerFactory.getLogger(FailoverClient.class);

    private static final int MAX_TRIES_PER_CANDIDATE = 2;


    /**
     * 单次上游调用。生产环境传 StreamingClient::stream，测试里传假实现
     */
    @FunctionalInterface
    public interface Upstream {
        Flux<StreamEvent> executeStream(ProtocolAdapter adapter, ChatRequest request, Duration firstByteTimeout);
    }

    private record Failure(String key, ErrorClass errorClass, String detail) {

    }

    private final Upstream upstream;
    private final CooldownRegistry cooldownRegistry;
    private final Duration firstByteBudget;
    private final Clock clock;

    public FailoverClient(Upstream upstream, CooldownRegistry cooldownRegistry, Duration firstByteBudget, Clock clock) {
        this.upstream = upstream;
        this.cooldownRegistry = cooldownRegistry;
        this.firstByteBudget = firstByteBudget;
        this.clock = clock;
    }
    //输入llm的模型名称和用户请求，输出StreamEvent 流
    public Flux<StreamEvent> streamWithFailover(List<Candidate> candidateList, ChatRequest request) {
        return Flux.defer(() -> {
            List<Candidate> available = candidateList.stream()
                    .filter(c -> !cooldownRegistry.isCooling(c.key()))
                    .toList();
            if (available.isEmpty()) {
                //全在冷却：不打上游，直接告诉客户端多久后再来
                long wait = cooldownRegistry.secondsUntilFirstAvailable(keys(candidateList));
                log.warn("所有候选都在冷却：model={} candidates={}", request.model(), keys(candidateList));
                return Flux.<StreamEvent>error(new UpstreamUnavailableException(
                        "All upstream provider are rate limited", true, wait));
            }
            Instant deadline = clock.instant().plus(firstByteBudget);
            return attempt(available, 0, 1, request, deadline, new ArrayList<>());
        });

    }

    /**
     * 尝试连接一个模型
      * @param candidateList 模型列表
     * @param index
     * @param tryNo
     * @param request
     * @param deadline
     * @param failures
     * @return
     */

    private Flux<StreamEvent> attempt(List<Candidate> candidateList, int index, int tryNo,
                                     ChatRequest request, Instant deadline, List<Failure> failures) {
        if (index >= candidateList.size()) {
            return Flux.error(exhausted(request, candidateList, failures));
        }

        Duration remaining = Duration.between(clock.instant(), deadline);

        if (remaining.isNegative() || remaining.isZero()) {
            failures.add(new Failure("-", ErrorClass.RETRYABLE,
                    "first-byte budget " + firstByteBudget.toSeconds() + "s exhausted"));
            return Flux.error(exhausted(request, candidateList, failures));
        }

        Candidate candidate = candidateList.get(index);
        // 别名换成这个候选的真实模型名，其余字段不变
        ChatRequest actual = new ChatRequest(candidate.model(), request.messages(), request.maxTokens());

        return upstream.executeStream(candidate.adapter(), actual, remaining)
                .onErrorResume(e -> {
                    // 上游 HTTP 错误交给这个厂商的 adapter 分类（各家状态码不一致），其余异常按通用规则
                    ErrorClass cls = e instanceof UpstreamHttpException http
                            ? candidate.adapter().classify(http)
                            : ErrorClass.of(e);
                    failures.add(new Failure(candidate.key(), cls ,summary(e)));
                    logFailure(request, candidate, tryNo, cls, e);

                    return switch(cls) {
                        case RETRYABLE -> tryNo < MAX_TRIES_PER_CANDIDATE
                                ? attempt(candidateList, index, tryNo + 1, request, deadline, failures)
                                : attempt(candidateList, index+1, 1, request, deadline, failures);

                        case RATE_LIMITED -> {
                            cooldownRegistry.markRateLimited(candidate.key(), ((UpstreamHttpException) e).retryAfter());
                            yield attempt(candidateList, index+1, 1, request, deadline, failures);
                        }

                        case PROVIDER_MISCONFIGURED ->
                                attempt(candidateList, index+1, 1, request, deadline, failures);

                        case CLIENT_ERROR, INTERNAL -> Flux.<StreamEvent>error(e);
                    };

                });
    }

    private UpstreamUnavailableException exhausted(ChatRequest request, List<Candidate> candidateList,
                                                   List<Failure> failures) {
        // 详细原因只写日志；返回给客户端的是简要说明，不暴露内部用了哪些厂商
        log.warn("所有候选都失败：model={} failures={}", request.model(), failures);
        boolean allRatelimited = !failures.isEmpty()
                && failures.stream().allMatch(f->f.errorClass() == ErrorClass.RATE_LIMITED);
        if (allRatelimited) {
            return new UpstreamUnavailableException("All upstream providers are rate limited", true,
                    cooldownRegistry.secondsUntilFirstAvailable(keys(candidateList)));
        }
        //如果不是rate limited 就报所有的失败
        return new UpstreamUnavailableException("All upstream providers failed", false, 0);

    }

    private void logFailure(ChatRequest request, Candidate c, int tryNo, ErrorClass cls, Throwable t) {
        String body = t instanceof UpstreamHttpException h ? h.body() : null;
        if (cls == ErrorClass.PROVIDER_MISCONFIGURED) {
            log.error("上游鉴权失败，检查 Key 配置：model={} candidate={} detail={} body={}",
                    request.model(), c.key(), summary(t), body);
        } else {
            log.warn("上游首字节前失败：model={} candidate={} try={} class={} detail={} body={}",
                    request.model(), c.key(), tryNo, cls, summary(t), body);
        }
    }

    private static String summary(Throwable t) {
        if (t instanceof UpstreamHttpException h) {
            return "HTTP " + h.statusCode();
        }
        // WebClientRequestException 外面一层信息量少，取最里层原因
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName();
    }

    private static List<String> keys(List<Candidate> candidates) {
        return candidates.stream().map(Candidate::key).collect(Collectors.toList());
    }



}
