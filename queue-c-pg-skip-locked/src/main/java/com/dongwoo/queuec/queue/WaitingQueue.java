package com.dongwoo.queuec.queue;

public interface WaitingQueue {
    String enqueue(String userId);
    long position(String token);
    int admitNext(int n);
    boolean isAdmitted(String token);
}
