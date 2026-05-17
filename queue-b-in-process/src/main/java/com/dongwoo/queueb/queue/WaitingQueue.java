package com.dongwoo.queueb.queue;

/**
 * 티켓팅 대기열 인터페이스.
 * - enqueue: 사용자 등록 → 토큰 반환
 * - position: 토큰의 현재 대기 순번 (1-based)
 * - admitNext: 디스패처가 앞에서 N명 통과시킴
 * - isAdmitted: 토큰이 통과된 상태인지 + TTL 유효한지
 */
public interface WaitingQueue {

    String enqueue(String userId);

    long position(String token);

    boolean admitNext(int n);

    boolean isAdmitted(String token);
}
