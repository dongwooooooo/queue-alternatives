# queue-alternatives

Stage 3 (대기열) 진입을 위해 [ticketing](https://github.com/dongwooooooo/ticketing) §6 에서 비교 검토할 3가지 대기열 구현 대안.

## 진입 배경

[seat-lock-alternatives](https://github.com/dongwooooooo/seat-lock-alternatives) 의 stress-baseline / stress-baseline-deep 측정으로 다음이 입증됨:
- Stage 2 베이스라인 (비관적 락 + partial UNIQUE) 은 race 정합성 OK
- 단, 5종 운영 부하 모드 (풀 고갈 / Deadlock / Lock timeout / Starvation / Connection leak) 에 취약
- → 대기열로 backend 직격 트래픽 차단 필요

## 대안 목록

| 디렉터리 | 큐 종류 | 핵심 |
|---|---|---|
| [queue-a-redis-sorted-set](queue-a-redis-sorted-set/) | Redis ZSET | Lua atomic ZADD/ZRANGE/ZREM, score = timestamp, 멀티 인스턴스 호환 |
| [queue-b-in-process](queue-b-in-process/) | LinkedBlockingQueue | Spring Bean, ConcurrentSkipListMap, 단일 노드 한정 |
| [queue-c-pg-skip-locked](queue-c-pg-skip-locked/) | PG queue table | `SELECT FOR UPDATE SKIP LOCKED` 디스패처 |

## 공통 시나리오

- 큐에 10,000 명 enqueue
- 디스패처가 통과 (각 큐별 페이싱·배치 정책 상이)
- 측정: enqueue throughput, admit throughput, p50/p99 enqueue latency, 인프라 의존성

## 측정 결과

| 큐 | enqueue ops/s | admit ops/s | p50 enqueue (ms) | p99 enqueue (ms) | elapsed (ms) | 인프라 의존 | 멀티 인스턴스 | 채택? |
|---|---|---|---|---|---|---|---|---|
| A. Redis ZSET | 28,571 | 1,095 (paced 1000/s) | 0.95 | 3.2 | 9,479 (페이싱 9초 포함) | Redis 추가 | OK | Stage 4 이월 |
| **B. in-process** | **181,818** | **2,000,000** | **0.009** | **1.27** | **60** | 없음 | NO | **Stage 3 채택** |
| C. PG SKIP LOCKED | 14,005 (32 threads) | 81,300 (8 worker × batch 50) | 0 | 16 | 837 | PG (이미 있음) | OK | 후보 (Stage 4) |

### 관찰 포인트

**A. Redis ZSET**
- 페이싱 1000/s 으로 admit 측정 — 실제 admit 처리량은 페이싱 한계가 1차 결정 요인
- p99 enqueue 3.2ms — 네트워크 round trip 1회 비용
- 한계: TTL은 키 단위 (admitted SET). 토큰별 정확 60s 만료 불가. NTP 점프 시 score 역전 가능 (엄격 단조성은 `INCR` 카운터로 가능)

**B. in-process**
- 모든 지표 최고. p99 1.27ms는 JVM 내부 큐 + lock-free 자료구조 비용
- `position()` 은 ConcurrentSkipListMap rank 미지원 → head 순회 O(n). 10K는 ms 단위 OK, 100K+ 부터 비용
- TTL 만료는 1초 주기 스케줄러 + lazy check (만료 직후 isAdmitted false 보장)
- 단일 JVM 한정 — 멀티 노드 환경에선 사용 불가

**C. PG SKIP LOCKED**
- enqueue 14K ops/s (단일 INSERT + UNIQUE index)
- admit 81K ops/s은 batch=50 효과 — batch=1로는 row당 비용 따로 측정 필요
- Spring Boot 4.0 + Flyway 11.14 autoconfig 미동작 (호환 이슈) → `spring.sql.init.schema-locations` 우회 필요. **운영 적용 전 호환 문제 해결 필요**
- 별도 인프라 불필요 (PG 이미 있음). 그러나 큐 트래픽이 DB 부하에 합산됨 — 베이스라인 한계 노출 영역에 추가 부하

## 채택 결정

**Stage 3 (단일 인스턴스 포트폴리오) 채택: B (in-process)**

이유:
- B는 다른 두 대안 대비 throughput·latency 압도적 (181K vs 28K vs 14K enqueue ops/s)
- Stage 3 목표는 backend 직격 트래픽 차단 — 외부 인프라 의존 추가 없이 달성 가능
- 단일 JVM 한정 = 포트폴리오 단일 인스턴스 가정과 일치
- 코드 단순 — Lua atomic 도 PG batch tuning 도 없음

**Stage 4 (멀티 인스턴스 확장) 이월: A 또는 C**

- A (Redis ZSET): 별도 Redis 가 이미 있다면 깔끔. Lua atomic 으로 multi-node 동시 admit 안전
- C (PG SKIP LOCKED): 별도 인프라 없음. 단, PG가 이미 베이스라인 한계 영역인데 큐 트래픽까지 합산되므로 측정·검증 필수. Spring Boot 4 호환 이슈 선결

Stage 3 → Stage 4 전환 시 `WaitingQueue` 인터페이스만 유지하고 impl 만 교체. 본 레포의 세 모듈 모두 같은 인터페이스를 따른다.

## 실행

```bash
./gradlew :queue-a-redis-sorted-set:test
./gradlew :queue-b-in-process:test
./gradlew :queue-c-pg-skip-locked:test
./gradlew test  # 전체
```
