package io.github.zxkgbwl.llmgateway.quota;

import io.github.zxkgbwl.llmgateway.tenant.Tenant;
import org.springframework.core.io.ClassPathResource;
import reactor.core.publisher.Mono;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

public class RedisTpmLimiter implements TpmLimiter {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> SCRIPT = RedisScript.of(new ClassPathResource("lua/tpm_bucket.lua"), List.class);

    private final ReactiveStringRedisTemplate redis;

    public RedisTpmLimiter(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }
    @Override
    public Mono<Void> reserve(Tenant tenant, int tokens) {
        return run(tenant, "reserve", tokens).flatMap(r-> {
            if (r.get(0) == 1L) {
                return Mono.<Void>empty();
            }
            long retryAfter = Math.max(1, (long) Math.ceil(r.get(2)/1000.0));
            return Mono.<Void>error(new QuotaExceededException(retryAfter, (long) r.get(1)));
        });

    }

    @Override
    public Mono<Void> adjust(Tenant tenant, int remainingTokens) {
        return remainingTokens == 0 ?  Mono.empty() : run(tenant, "adjust", remainingTokens).then();
    }

    @SuppressWarnings("unchecked")
    private Mono<List<Long>> run(Tenant tenant, String mode, int n) {
        List<String> keys = List.of("tpm:{" + tenant.id() + "}");
        List<String> args = List.of(String.valueOf(tenant.tpm()), String.valueOf(n), mode);
        return redis.execute(SCRIPT, keys, args).next().map(r->(List<Long>) r);
    }
}
