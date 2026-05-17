package com.dongwoo.queueb.queue;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process 대기열 구현.
 *
 * 자료구조:
 * - waiting: ConcurrentSkipListMap<sequence, token>
 *   → enqueue 시 단조 증가 sequence를 key로 사용, deque 대신 skiplist 로 position 조회 O(log n).
 * - tokenToSeq: token → sequence 역인덱스 (position 조회용).
 * - admitted: token → admit 시각 (TTL 체크용).
 *
 * 멀티스레드:
 * - sequence는 AtomicLong, waiting/tokenToSeq/admitted는 모두 thread-safe map.
 * - admitNext 는 pollFirstEntry 를 N번 반복 — 동시 호출 시에도 각 엔트리는 단일 caller에만 노출됨.
 */
@Component
public class InProcessWaitingQueue implements WaitingQueue {

    private final ConcurrentSkipListMap<Long, String> waiting = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<String, Long> tokenToSeq = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> admitted = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong(0);

    private final long admitTtlMillis;

    private ScheduledExecutorService cleaner;

    public InProcessWaitingQueue(@Value("${queue.admit-ttl-seconds:60}") long admitTtlSeconds) {
        this.admitTtlMillis = TimeUnit.SECONDS.toMillis(admitTtlSeconds);
    }

    @PostConstruct
    void start() {
        cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "queue-b-admit-cleaner");
            t.setDaemon(true);
            return t;
        });
        cleaner.scheduleAtFixedRate(this::evictExpiredAdmissions, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    void stop() {
        if (cleaner != null) {
            cleaner.shutdownNow();
        }
    }

    @Override
    public String enqueue(String userId) {
        String token = userId + ":" + UUID.randomUUID();
        long seq = sequence.incrementAndGet();
        waiting.put(seq, token);
        tokenToSeq.put(token, seq);
        return token;
    }

    /**
     * 토큰의 현재 대기 순번 (1-based). 큐에 없으면 0.
     * ConcurrentSkipListMap.headMap(seq).size() — O(n) worst case 지만
     * Java 의 ConcurrentSkipListMap.size() 는 O(n). 정확한 1-based 위치를 위해 순회.
     * 더 빠른 방법: 별도 카운터 / RankList 가 필요하지만 in-process 데모에선 충분.
     */
    @Override
    public long position(String token) {
        Long seq = tokenToSeq.get(token);
        if (seq == null) {
            return 0L;
        }
        // headMap(seq, true) 의 size: skiplist 에서는 O(n). 명시적 카운트.
        long rank = 0L;
        for (Long key : waiting.navigableKeySet()) {
            rank++;
            if (key.equals(seq)) {
                return rank;
            }
            if (key > seq) {
                // seq 가 이미 제거된 경우 (admit 됨)
                return 0L;
            }
        }
        return 0L;
    }

    @Override
    public boolean admitNext(int n) {
        if (n <= 0) {
            return false;
        }
        int admittedCount = 0;
        long now = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Map.Entry<Long, String> entry = waiting.pollFirstEntry();
            if (entry == null) {
                break;
            }
            String token = entry.getValue();
            tokenToSeq.remove(token);
            admitted.put(token, now);
            admittedCount++;
        }
        return admittedCount > 0;
    }

    @Override
    public boolean isAdmitted(String token) {
        Long admittedAt = admitted.get(token);
        if (admittedAt == null) {
            return false;
        }
        if (System.currentTimeMillis() - admittedAt > admitTtlMillis) {
            admitted.remove(token, admittedAt);
            return false;
        }
        return true;
    }

    /**
     * 테스트용 초기화.
     */
    public void clear() {
        waiting.clear();
        tokenToSeq.clear();
        admitted.clear();
        sequence.set(0);
    }

    public int waitingSize() {
        return waiting.size();
    }

    public int admittedSize() {
        return admitted.size();
    }

    private void evictExpiredAdmissions() {
        long now = System.currentTimeMillis();
        admitted.entrySet().removeIf(e -> now - e.getValue() > admitTtlMillis);
    }
}
