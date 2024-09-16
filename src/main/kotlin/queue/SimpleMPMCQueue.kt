package com.esh.jbr.queue

import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.atomicfu.locks.withLock

class SimpleMPMCQueue<E> {
    private val queue: ArrayDeque<E> = ArrayDeque(0)
    private val lock: ReentrantLock = ReentrantLock()

    fun enqueue(element: E): Unit {
        lock.withLock {
            queue.addLast(element)
        }
    }

    fun dequeue(): E? {
        lock.withLock {
            return queue.removeFirst()
        }
    }
}