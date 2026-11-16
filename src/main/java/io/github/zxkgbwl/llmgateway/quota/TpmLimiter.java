package io.github.zxkgbwl.llmgateway.quota;

import io.github.zxkgbwl.llmgateway.tenant.Tenant;
import reactor.core.publisher.Mono;

public interface TpmLimiter {

    /** 原子地预扣 tokens 个 token；余额不够时以 QuotaExceededException 结束。 */
    Mono<Void> reserve(Tenant tenant, int tokens);

    /** 结算差额：正数退回，负数补扣。补扣允许余额为负（欠账）。 */
    Mono<Void> adjust(Tenant tenant, int remainingToken);
}
