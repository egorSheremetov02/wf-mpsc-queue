package com.esh.jbr.queue

import kotlinx.atomicfu.AtomicLong
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

class WaitFreeMPSCQueue<T>(bufferSizeLog: Int) {
    private enum class CellState {
        EMPTY, READY, DEQUEUED
    }

    private enum class IndexInfo {
        BEFORE, AFTER, WITHIN
    }

    private class ArrayCell<T> {
        val state = atomic(CellState.EMPTY)
        var elem: T? = null  // potentially could use Optional instead

        fun unwrap(): T = elem!!

        fun setValue(value: T) {
            this.elem = value
            this.state.value = CellState.READY
        }
    }

    private class Buffer<T>(val sizeLog: Int, val index: Long) {

        val data: Array<ArrayCell<T>> = Array(1 shl sizeLog) { ArrayCell() }
        var prevBuffer: Buffer<T>? = null
        val nextBuffer: AtomicRef<Buffer<T>?> = atomic(null)

        fun indexInfo(idx: Long): IndexInfo {
            val startIndex = index shl sizeLog
            val endIndex = (index + 1) shl sizeLog  // exclusive
            if (idx < startIndex) {
                return IndexInfo.BEFORE
            }
            if (idx >= endIndex) {
                return IndexInfo.AFTER
            }
            return IndexInfo.WITHIN
        }

        fun internalIndex(idx: Long): Int = (idx and ((1L shl sizeLog) - 1)).toInt()

        fun assign(idx: Long, element: T): Unit = read(idx).setValue(element)

        fun read(idx: Long): ArrayCell<T> = data[internalIndex(idx)]

        fun fold() {
            val next = nextBuffer.value
            assert(next != null)
            // need to make sure that we don't reference folded element from the list
            next!!.prevBuffer = null
        }

        fun prepareNext(): Buffer<T> {
            val nextBuffer = Buffer<T>(sizeLog, index + 1)
            nextBuffer.prevBuffer = this
            return nextBuffer
        }
    }

    private var head = 0L  // single producer is the only one accessing it, thus safe to leave without volatile/atomic
    private val tail: AtomicLong = atomic(0L)

    private var consumerBuffer: Buffer<T> = Buffer(bufferSizeLog, 0)
    private val producerBuffer: AtomicRef<Buffer<T>> = atomic(consumerBuffer)

    fun enqueue(item: T) {
        val itemPos = tail.getAndIncrement()
        var buffer = producerBuffer.value

        // We need to create new buffers if the new index can't fit into the current ones
        while (buffer.indexInfo(itemPos) == IndexInfo.AFTER) {  // itemPos should be inserted somewhere after the current buffer

            val next = buffer.nextBuffer.value

            // can't be just != null check as we also need to make sure we haven't made it available
            // for a garbage collection (which means we have to set next to an auxiliary value)
            if (next != null) {
                producerBuffer.compareAndSet(buffer, next)
            } else {  // then we try to create a next element
                val nextBuffer = buffer.prepareNext()
                if (buffer.nextBuffer.compareAndSet(null, nextBuffer)) {
                    producerBuffer.compareAndSet(buffer, nextBuffer)
                }
            }

            // in case next == RemovedFromList.INSTANCE we re-read the buffer and should be able to get a fresh value
            // that is not removed
            buffer = producerBuffer.value
        }

        var wentBackwards = false
        // INVARIANT: buffer.indexInfo(itemPos) != IndexInfo.AFTER
        while (buffer.indexInfo(itemPos) == IndexInfo.BEFORE) {  // we should be somewhere before
            buffer = buffer.prevBuffer!!  // should be safe to do, as we would only fold this previous buffer in case
            // we had all the elements from it dequeued
            wentBackwards = true
        }

        // INVARIANT: buffer.indexInfo(itemPos) == IndexInfo.WITHIN
        buffer.assign(itemPos, item)

        if (buffer.internalIndex(itemPos) == 0 && !wentBackwards) {
            val nextBuffer = buffer.prepareNext()
            // for this given buffer only one thread was allocated 0-th element, hence ideally this operation should
            // be executed without any contention (in practice)
            buffer.nextBuffer.compareAndSet(null, nextBuffer)
        }
    }

    fun dequeue(): T? {
        // skip dequeued elements
        val curTail = tail.value
        while (head < curTail) {

            // we should jump to the next buffer
            if (consumerBuffer.indexInfo(head) == IndexInfo.AFTER) {
                // elvis operator here means that we got to the end of the current buffer but there is still no
                // next one set; in this case the best we can do is saying that the buffer is empty
                // we certainly don't want to block here
                val next = consumerBuffer.nextBuffer.value ?: return null

                val oldConsumer = consumerBuffer

                // here we are sure that next != RemovedFromList.INSTANCE as only consumer thread has the ability
                // to set nextBuffer to RemovedFromList.INSTANCE (to help the GC by detaching it from the list)
                // thus, this cast should be correct
                consumerBuffer = next
                if (producerBuffer.value == oldConsumer) {  // sorta helping but not really
                    producerBuffer.compareAndSet(oldConsumer, next)
                }
                oldConsumer.fold()  // this is absolutely safe as there can't be any other thread that
                // truly wants anything from this buffer, some stale producers might have a reference to such a buffer
                // however, it means that they should re-read the producerBuffer value because they had an outdated view
            }

            // INVARIANT: is made sure of by the previous if statement, we can never be ahead of the current
            // consumerBuffer by more than 1 element, so jumping through just one next is sufficient to get into the
            // right buffer
            assert(consumerBuffer.indexInfo(head) == IndexInfo.WITHIN)
            if (consumerBuffer.read(head).state.value == CellState.DEQUEUED) {
                // here we possibly break the invariant, this is fine though, we will fix that on our next loop iteration
                head += 1
            } else {
                break  // exactly the point at which we ran out of DEQUEUED elements
            }
        }

        // INVARIANT: must be true, just take a look when we leave the previous while(true) loop
        assert(consumerBuffer.indexInfo(head) == IndexInfo.WITHIN)

        if (head == curTail) {  // all elements up to the tail are dequeued
            return null
        }

        // means the enqueue operation is still running, we don't want to get block on this though
        // let's scan through the next elements to see if that is going to help
        if (consumerBuffer.read(head).state.value == CellState.EMPTY) {
            val emptyCells: MutableList<ArrayCell<T>> =
                mutableListOf()  // empty cells that we have encountered on the way
            // to the first READY element that we could find

            var curBuffer = consumerBuffer
            var curIndex = head
            var curCell = curBuffer.read(curIndex)
            while (curCell.state.value != CellState.READY) {
                if (curCell.state.value == CellState.EMPTY) {
                    emptyCells.add(curCell)
                }

                ++curIndex

                if (curIndex == curTail) {
                    // scanned up to curTail and found nothing
                    return null
                }

                if (curBuffer.indexInfo(curIndex) == IndexInfo.AFTER) {
                    val next = curBuffer.nextBuffer.value
                        ?: return null  // we have no further to scan, and all up to now are still in the insertion process

                    curBuffer = next
                }

                curCell = curBuffer.read(curIndex)
            }

            // INVARIANT: must be true as this is the only condition under which we escape the loop
            // but not the entire dequeue function
            assert(curCell.state.value == CellState.READY)

            // now we need to do double scan, as we have to make sure we conform to a linearizability requirement
            var cellToDequeue = curCell
            var run = true
            while (run) {  // always completes after bounded number of iterations
                run = false
                var emptyPrefixSize = 0
                for (cell in emptyCells) {
                    if (cell.state.value == CellState.EMPTY) {
                        emptyPrefixSize++
                    } else {
                        cellToDequeue = curCell
                        // if we found an element earlier than previous one, we need to scan one more time
                        run = true
                    }
                }
                while (emptyCells.size > emptyPrefixSize) {
                    emptyCells.removeLast()
                }
            }

            // this is done just so it is easier for us to understand when to advance head in this scenario
            if (consumerBuffer.read(head).state.value == CellState.READY) {
                cellToDequeue = consumerBuffer.read(head)
                ++head
            }
            // INVARIANT
            assert(cellToDequeue.state.value == CellState.READY)
            cellToDequeue.state.value = CellState.DEQUEUED
            return cellToDequeue.unwrap()
        }

        val cellToDequeue = consumerBuffer.read(head)
        // INVARIANT
        assert(cellToDequeue.state.value == CellState.READY)
        cellToDequeue.state.value = CellState.DEQUEUED
        ++head
        return cellToDequeue.unwrap()
    }
}
