package io.github.zxkgbwl.llmgateway.provider;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 记录上游的429 信息
 *
 */
public final class CooldownRegistry {

    private static final Logger log = LoggerFactory.getLogger(CooldownRegistry.class.getName());

    private final Map<String, Instant> until = new ConcurrentHashMap<>();

    private final Clock clock;

    private final Duration defaultCooldown;

    /** 记一次 429。冷却时长优先取 Retry-After，没有或解析失败就用默认值 */
    public CooldownRegistry(Clock clock, Duration defaultCooldown) {
        this.clock = clock;
        this.defaultCooldown = defaultCooldown;
    }

    /** 记一次 429。冷却时长优先取 Retry-After，没有或解析失败就用默认值 */
    public void markRateLimited(String key, String retryAfterHeader) {
        Duration duration = parseRetryAfter(retryAfterHeader).orElse(defaultCooldown);
        Instant end = clock.instant().plus(duration);
        // 并发下多次 429，取更晚的结束时间
        until.merge(key, end, (a, b) -> a.isAfter(b) ? a : b);
        log.warn("上游限流，进入冷却：key={} duration={}s", key, duration.toSeconds());
    }

    public boolean isCooling(String key) {
        Instant end = until.get(key);
        if (end == null) {
            return false;
        }
        if (clock.instant().isBefore(end)) {
            return true;
        }
        until.remove(key, end); // 到期顺手清掉
        return false;
    }

    /** 这些候选里最早恢复的那个还要等多少秒（向上取整，至少 1 秒），用作返回给客户端的 Retry-After */
    public long secondsUntilFirstAvailable(Collection<String> keys) {
        Instant now = clock.instant();
        long minMillis = keys.stream()
                .map(until::get)
                .filter(end -> end != null && end.isAfter(now))
                .mapToLong(end -> Duration.between(now, end).toMillis())
                .min()
                .orElse(1000);
        return Math.max(1, (minMillis + 999) / 1000);
    }

    /** Retry-After 有两种格式：秒数，或 HTTP 日期 */
    Optional<Duration> parseRetryAfter(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        String v = value.trim();
        try {
            long seconds = Long.parseLong(v);
            return seconds > 0 ? Optional.of(Duration.ofSeconds(seconds)) : Optional.empty();
        } catch (NumberFormatException ignored) {
            // 不是秒数，再试 HTTP 日期
        }
        try {
            Instant at = ZonedDateTime.parse(v, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            Duration d = Duration.between(clock.instant(), at);
            return d.isNegative() || d.isZero() ? Optional.empty() : Optional.of(d);
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

}
