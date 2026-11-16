package io.github.zxkgbwl.llmgateway.tenant;


/**
 * 租户。本周只放限流需要的字段。
 * keySha256 是虚拟 Key 的 SHA-256（小写十六进制），配置里不存明文 Key。
 */
public record Tenant(String id, String keySha256, int tpm, int defaultMaxTokens) {
    /** 鉴权通过后，租户放在 exchange attributes 里的键 */
    public static final String ATTR = Tenant.class.getName();
}
