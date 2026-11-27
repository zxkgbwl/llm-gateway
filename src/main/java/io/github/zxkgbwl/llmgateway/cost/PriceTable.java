package io.github.zxkgbwl.llmgateway.cost;

import io.github.zxkgbwl.llmgateway.provider.Candidate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class PriceTable {
    public static final PriceTable EMPTY = new PriceTable(List.of());

    public record Price(BigDecimal input, BigDecimal output, BigDecimal cacheRead) {
        public Price {
            if (cacheRead == null) cacheRead = input;
        }

        /** 美元/百万 token × token 数 = 微美元，再 ×1000 得纳美元 */
        public long costNanoUsd(int inputTokens, int outputTokens, int cacheReadTokens) {
            int uncached = Math.max(0, inputTokens - cacheReadTokens);
            BigDecimal micro = input.multiply(BigDecimal.valueOf(uncached))
                    .add(cacheRead.multiply(BigDecimal.valueOf(cacheReadTokens)))
                    .add(output.multiply(BigDecimal.valueOf(outputTokens)));
            return micro.movePointRight(3).setScale(0, RoundingMode.HALF_UP).longValueExact();
        }
    }

    private final Map<String, Price> byKey = new HashMap<>();

    public PriceTable(List<PricingProperties.ModelPrice> prices) {
        for (PricingProperties.ModelPrice p : prices) {
            String key = p.provider() + ":" + p.model();
            // 配重复了启动即失败，免得悄悄用错价格
            if (byKey.put(key, new Price(p.inputPerMtok(), p.outputPerMtok(), p.cacheReadPerMtok())) != null) {
                throw new IllegalStateException("Duplicate price for " + key);
            }
        }
    }

    public Optional<Price> find(Candidate candidate) {
        return Optional.ofNullable(byKey.get(candidate.key()));
    }
}
