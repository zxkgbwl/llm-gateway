package io.github.zxkgbwl.llmgateway.tenant;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;


@ConfigurationProperties(prefix = "gateway")
public record TenantProperties(List<Tenant> tenants) {
    public TenantProperties {
        tenants = tenants == null ? new ArrayList<>() : List.copyOf(tenants);
    }



}
