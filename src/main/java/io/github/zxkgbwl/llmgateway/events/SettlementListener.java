package io.github.zxkgbwl.llmgateway.events;

@FunctionalInterface
public interface SettlementListener {
    SettlementListener NONE = e -> {};

    void onSettled(SettlementEvent event);
}
