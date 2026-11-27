package io.github.zxkgbwl.llmgateway.cost;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.util.List;

/** gateway.pricing：按 provider + model 配每百万 token 的美元价。 */
@ConfigurationProperties(prefix = "gateway")
public record PricingProperties(List<ModelPrice> pricing) {

    /** cacheReadPerMtok 不填时按输入价 */
    public record ModelPrice(String provider, String model,
                             BigDecimal inputPerMtok, BigDecimal outputPerMtok, BigDecimal cacheReadPerMtok) {}
}
