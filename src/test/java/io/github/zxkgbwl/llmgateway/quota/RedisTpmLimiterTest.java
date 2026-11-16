package io.github.zxkgbwl.llmgateway.quota;

import io.github.zxkgbwl.llmgateway.tenant.Tenant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Lua 脚本的原子语义，用真 Redis 验证（需要 Docker Desktop 在运行）。 */
@Testcontainers
class RedisTpmLimiterTest {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static LettuceConnectionFactory factory;
    static RedisTpmLimiter limiter;

    @BeforeAll
    static void setUp() {
        factory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        factory.afterPropertiesSet();
        factory.start();
        limiter = new RedisTpmLimiter(new ReactiveStringRedisTemplate(factory));
    }

    @AfterAll
    static void tearDown() {
        factory.destroy();
    }

    @Test
    void reserveRejectsWhenShort_andRefundRestores() {
        Tenant tenant = new Tenant("t-" + System.nanoTime(), "unused", 1000, 100);

        StepVerifier.create(limiter.reserve(tenant, 600)).verifyComplete();

        // 余额约 400，再要 600 被拒，并给出等待时间
        StepVerifier.create(limiter.reserve(tenant, 600))
                .expectErrorSatisfies(e -> assertTrue(
                        e instanceof QuotaExceededException q && q.retryAfterSeconds() >= 1))
                .verify();

        // 结算退回 500 后够了
        StepVerifier.create(limiter.adjust(tenant, 500)).verifyComplete();
        StepVerifier.create(limiter.reserve(tenant, 600)).verifyComplete();
    }
}