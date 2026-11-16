package io.github.zxkgbwl.llmgateway.tenant;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TenantRegistry {
    private final Map<String, Tenant> byKeyHash;

    public TenantRegistry(List<Tenant> tenants) {
        this.byKeyHash = tenants.stream().collect(Collectors.toUnmodifiableMap(
                t -> t.keySha256().toLowerCase(Locale.ROOT), Function.identity()));
    }

    public Optional<Tenant> findByKey(String rawKey) {
        return Optional.ofNullable(byKeyHash.get(sha256(rawKey)));
    }

    static String sha256(String text) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e); // JDK 必带 SHA-256，不会发生
        }
    }
}
