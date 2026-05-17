# queue-b-in-process

티켓팅 대기열의 **in-process** 구현. 외부 의존(Redis, DB) 없이 JVM 메모리 안에서 동작한다.

## 자료구조

- `ConcurrentSkipListMap<Long, String> waiting` — key=enqueue sequence, value=token
- `ConcurrentHashMap<String, Long> tokenToSeq` — token → sequence (position 조회용 역인덱스)
- `ConcurrentHashMap<String, Long> admitted` — token → admit 시각 (TTL 체크용)
- `AtomicLong sequence` — 단조 증가 sequence 발급

## 동작

| 메서드 | 동작 | 복잡도 |
|---|---|---|
| `enqueue(userId)` | sequence 발급 → waiting/tokenToSeq put | O(log n) |
| `position(token)` | tokenToSeq → seq, waiting head 순회 | O(n) worst |
| `admitNext(n)` | `pollFirstEntry` N번, admitted put | O(n log m) |
| `isAdmitted(token)` | admitted lookup + TTL 체크 | O(1) |

## 한계점

- `position`은 `ConcurrentSkipListMap`의 rank 조회가 O(n)이라 큰 큐에선 비싸다. RankList(예: indexed skiplist) 자료구조가 필요하면 별도 구현 필요.
- TTL 만료는 1초 주기 스케줄러에서 청소. 정확한 만료 직후 isAdmitted는 false를 반환한다.
- 단일 JVM 한계 — 멀티 프로세스/노드 분산 환경에는 Redis(queue-a) 또는 PG(queue-c) 사용.

## 실행

```bash
../gradlew :queue-b-in-process:test
```
