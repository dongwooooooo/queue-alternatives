# queue-a-redis-sorted-set

티켓팅 대기열의 **Redis Sorted Set 기반** 구현.

## 동작 방식

### 자료구조

| 키 | 타입 | 용도 |
|----|------|------|
| `queue:waiting` | ZSET | 대기 중인 토큰. score = enqueue 시각(epoch millis) |
| `queue:admitted` | SET | 통과 처리된 토큰. 키 단위 TTL 60초 |

### 토큰 포맷

`UUID + ":" + userId` — UUID 로 고유성, userId 는 디버깅 용도.

### 핵심 연산

1. **enqueue**: `ZADD queue:waiting <epochMillis> <token>` — 단일 O(log N) 명령.
2. **position**: `ZRANK` 후 +1 로 1-based 변환. 부재 시 `SISMEMBER queue:admitted` 로 `ALREADY_ADMITTED` / `NOT_FOUND` 구분.
3. **admitNext(n)**: Lua 스크립트 1회 호출로 원자 처리.
   - `ZRANGE queue:waiting 0 N-1` 으로 앞 N 개 추출
   - 각 토큰을 `SADD queue:admitted` 한 뒤 `ZREM queue:waiting`
   - 마지막에 `EXPIRE queue:admitted 60` 으로 키 TTL 갱신
4. **isAdmitted**: `SISMEMBER` — O(1).

### 원자성

`admitNext` 는 Lua 스크립트로 묶여 있으므로 ZRANGE 와 ZREM 사이에 다른 클라이언트가 끼어들 수 없음. 디스패처가 여러 인스턴스라도 같은 토큰을 두 번 통과시키지 않는다.

## 장점

- **수평 확장**: Redis 한 대 = 1초당 10만+ 명령 처리. enqueue/position 은 클라이언트 측 부담 거의 없음.
- **공정성**: score 가 enqueue 시각이므로 FIFO. 동시 enqueue 시에도 epochMillis 분해능 내에서 결정적.
- **TTL 자동 청소**: 통과 SET 이 60초 후 자동 만료. 별도 GC 불필요.
- **연산 단순**: Lua 한 줄로 atomic dispatch.

## 단점

- **단일 SPOF**: Redis 마스터가 죽으면 큐 전체 정지. 복제 + Sentinel/Cluster 필요.
- **persistence trade-off**: RDB 만 켜면 마지막 N 초 데이터 손실. AOF 켜면 latency 상승.
- **SET 멤버 TTL 불가**: 통과 토큰의 만료는 SET 키 단위로만 제어 가능. 신규 통과가 계속 들어오면 EXPIRE 가 갱신되어 오래된 통과 토큰도 함께 60초 더 유지됨. 즉, "토큰별 정확히 60초" 가 아니라 "마지막 dispatch 이후 60초" 에 가까움. 토큰별 TTL 이 필요하면 SET 대신 토큰별 `SETEX queue:admitted:<token>` 키로 전환해야 함.
- **score 단조성**: `System.currentTimeMillis` 가 NTP 점프하면 순서 역전 가능. 엄격한 단조성이 필요하면 `INCR` 카운터를 score 로 쓰는 편이 안전.
- **메모리 비용**: 1M 사용자 = ZSET ~80MB + SET ~50MB. ZSET 의 skiplist 가 비싸다.
- **position 정확도**: ZRANK 는 O(log N) 이지만 1M 동시 호출 시 Redis 단일 스레드 병목. 클라이언트 캐시 / 폴링 주기 조절 필요.

## 측정 결과

`/Users/idong-u/d/queue-alternatives/gradlew :queue-a-redis-sorted-set:test` 결과는 `test-output.txt` 참조. 결과 헤더:

```
===== QUEUE-A RESULT =====
enqueueCount=10000
admittedCount=10000
elapsedMs=<n>
enqueueOpsPerSec=<n>
admitOpsPerSec=<n>
p50EnqueueMs=<n>
p99EnqueueMs=<n>
==========================
```

테스트 시나리오:
- 10,000 명을 32 스레드로 동시 enqueue
- 디스패처가 1초 간격으로 1,000 명씩 admitNext — 10 회 반복
- 모든 토큰의 isAdmitted = true, waitingCount = 0 검증
- enqueue latency 의 p50 / p99 측정

## 한계 및 후속 작업

- **dispatcher backpressure**: 본 테스트는 페이싱 sleep 으로 1k/s 를 흉내냈지만, 실제로는 admit 후 좌석 락 점유율을 보고 가변 속도로 흘려야 함.
- **다중 큐**: 공연/회차별로 키 분리 (`queue:waiting:concert-42`) 가 필요. 현 구현은 단일 큐.
- **취소 처리**: 사용자가 대기 중 이탈 시 `ZREM queue:waiting <token>` 으로 즉시 제거. position 이 자동으로 당겨짐.
