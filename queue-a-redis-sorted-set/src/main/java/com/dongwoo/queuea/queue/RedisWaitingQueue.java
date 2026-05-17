package com.dongwoo.queuea.queue;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/**
 * Redis Sorted Set 기반 대기열 구현.
 *
 * 키 구성:
 * - queue:waiting  (ZSET, score=enqueue epoch millis)
 * - queue:admitted (SET, 통과된 토큰 TTL 60s)
 */
@Component
public class RedisWaitingQueue implements WaitingQueue {

    public static final String WAITING_KEY = "queue:waiting";
    public static final String ADMITTED_KEY = "queue:admitted";
    public static final long ADMITTED_TTL_SECONDS = 60L;

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<Long> admitScript;

    public RedisWaitingQueue(StringRedisTemplate redis) {
        this.redis = redis;
        // Lua: 앞에서 N개 ZRANGE → 각 토큰 SADD admitted + ZREM waiting.
        // admitted set 의 TTL 은 SET 키 단위로만 가능하므로 EXPIRE 로 재설정.
        // 토큰별 TTL 은 SET 멤버에 불가능 → 키 단위 TTL 로 근사 (요구사항 60s 유지).
        String lua = """
            local waiting = KEYS[1]
            local admitted = KEYS[2]
            local n = tonumber(ARGV[1])
            local ttl = tonumber(ARGV[2])
            if n <= 0 then return 0 end
            local tokens = redis.call('ZRANGE', waiting, 0, n - 1)
            if #tokens == 0 then return 0 end
            for i = 1, #tokens do
              redis.call('SADD', admitted, tokens[i])
              redis.call('ZREM', waiting, tokens[i])
            end
            redis.call('EXPIRE', admitted, ttl)
            return #tokens
            """;
        this.admitScript = new DefaultRedisScript<>(lua, Long.class);
    }

    @Override
    public String enqueue(String userId) {
        String token = UUID.randomUUID() + ":" + userId;
        long score = System.currentTimeMillis();
        redis.opsForZSet().add(WAITING_KEY, token, score);
        return token;
    }

    @Override
    public long position(String token) {
        Long rank = redis.opsForZSet().rank(WAITING_KEY, token);
        if (rank != null) {
            return rank + 1L;
        }
        Boolean admitted = redis.opsForSet().isMember(ADMITTED_KEY, token);
        if (Boolean.TRUE.equals(admitted)) {
            return ALREADY_ADMITTED;
        }
        return NOT_FOUND;
    }

    @Override
    public boolean admitNext(int n) {
        List<String> keys = List.of(WAITING_KEY, ADMITTED_KEY);
        Long admitted = redis.execute(
                admitScript,
                keys,
                Integer.toString(n),
                Long.toString(ADMITTED_TTL_SECONDS));
        return admitted != null && admitted > 0;
    }

    @Override
    public boolean isAdmitted(String token) {
        return Boolean.TRUE.equals(redis.opsForSet().isMember(ADMITTED_KEY, token));
    }

    /** 테스트용: 상태 초기화. */
    public void reset() {
        redis.delete(List.of(WAITING_KEY, ADMITTED_KEY));
    }

    /** 통과된 누적 카운트는 SET 크기로 근사 (TTL 만료 전 한정). */
    public long admittedCount() {
        Long size = redis.opsForSet().size(ADMITTED_KEY);
        return size == null ? 0L : size;
    }

    /** 현재 대기 중인 인원 수. */
    public long waitingCount() {
        Long size = redis.opsForZSet().size(WAITING_KEY);
        return size == null ? 0L : size;
    }

    /** 빈 리스트 보호용. */
    @SuppressWarnings("unused")
    private static <T> List<T> safe(List<T> list) {
        return list == null ? Collections.emptyList() : list;
    }
}
