package com.ceronman.jetindexer

import java.nio.ByteBuffer

data class Posting(val docId: Int, val position: Int)

internal class PostingList {
    private var buffer = ByteBuffer.allocate(1024)
    private val growthFactor = 1.5

    fun position(): Int {
        return buffer.position()
    }

    fun close(): ByteBuffer {
        growIfNeeded(VAR_INT_MAX_SIZE)
        buffer.putVarInt(0)
        buffer.flip()
        return buffer.asReadOnlyBuffer()
    }

    fun put(docId: Int, positions: List<Int>): Int {
        growIfNeeded(VAR_INT_MAX_SIZE * 2)
        val startPosition = buffer.position()
        buffer.putVarInt(docId)
        buffer.putVarInt(positions.size)
        var prev = 0
        for (position in positions) {
            growIfNeeded(VAR_INT_MAX_SIZE)
            buffer.putVarInt(position - prev)
            prev = position
        }
        return buffer.position() - startPosition
    }

    private fun growIfNeeded(required: Int) {
        if (buffer.remaining() < required) {
            val newBuffer = ByteBuffer.allocate((buffer.capacity() * growthFactor).toInt())
            buffer.flip()
            newBuffer.put(buffer)
            buffer = newBuffer
        }
    }
}

internal class PostingListView(private val buffers: List<ByteBuffer>) {

    fun peekableIterator(): PostingViewIterator {
        return PostingViewIterator(readPostings().iterator())
    }

    class PostingViewIterator(private val iter: Iterator<Posting>) {
        private var current: Posting?

        init {
            if (iter.hasNext()) {
                current = iter.next()
            } else {
                current = null
            }
        }

        fun hasNext(): Boolean {
            return iter.hasNext()
        }

        fun next(): Posting {
            val old = current
            current= iter.next()
            return old!!
        }

        fun peek(): Posting {
            return current!!
        }
    }

    private fun readPostings(): Sequence<Posting> = sequence {
        for (buffer in buffers) {
            var docId = buffer.getVarInt()
            while (docId != 0) {
                val numPositions = buffer.getVarInt()
                var prev = 0
                for (j in 0 until numPositions) {
                    val position = buffer.getVarInt() + prev
                    yield(Posting(docId, position))
                    prev = position
                }
                docId = buffer.getVarInt()
            }
        }
    }
}