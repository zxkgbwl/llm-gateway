package io.github.zxkgbwl.llmgateway.events;


/**
 * 一次结算的事件：token 对账 + 成本。发到 Kafka，key = tenantId。
 *
 * <p>时间用 epoch 毫秒，避免序列化格式的分歧；成本单位纳美元，未计价时为 null。
 */
public record SettlementEvent(
        String requestId,
        long timestampMs,
        String tenantId,
        String model,          // 客户端请求的模型名（可能是别名）
        String provider,       // 实际服务的候选 provider:model，没打到上游为 none
        String outcome,        // COMPLETE / ERROR / CANCEL
        String source,         // USAGE / ESTIMATED / REFUNDED
        int reserved,
        int inputTokens,
        int outputTokens,
        int cacheReadTokens,
        Long costNanoUsd,
        boolean costEstimated) {
}