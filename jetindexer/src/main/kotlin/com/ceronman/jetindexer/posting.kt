// Copyright Manuel Cerón. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.ceronman.jetindexer

import java.nio.ByteBuffer

/**
 * Represents the position of a token in a specific document.
 * The position is the position of the token since the beginning of the file.
 */
data class Posting(val docId: Int, val position: Int)

/**
 * Represents a list of postings for a given token.
 *
 * An inverted index maps a token to a posting list. The posting lists contains
 * the document IDs that have the token and the positions of that token in the files.
 * This is the heart of the inverted index.
 *
 * To be memory efficient, this stores the posting list in a compressed form inside a
 * [ByteBuffer].
 *
 * (document id)(number of positions)(pos 1)(pos 2)...(pos n)(doc id)...
 *
 * The integers representing the document ids, number of positions and positions are encoded
 * in a variable byte encoding (aka VarInt) (@see [ByteBuffer.putVarInt]) to save space.
 *
 * To increase efficiency, the positions are stored as deltas. So for example of for a given
 * file the list of positions [1 7 20]. These are stored as [1, 6, 14]. Smaller integers allow
 * for better compression.
 *
 * The order of the posting list is very important. File IDs should be stored in a monotonically
 * increasing way. Same for file positions.
 */
internal class PostingList {
    private var buffer = ByteBuffer.allocate(1024)
    private val growthFactor = 1.5

    /**
     * Returns a read-only version of the internal [ByteBuffer] representing this posting list.
     */
    fun asReadOnly(): ByteBuffer {
        val buf = buffer.duplicate()
        buf.flip()
        return buf.asReadOnlyBuffer()
    }

    /**
     * Stores a document ID and a list of positions for a given token in the list
     */
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

    /**
     * Convenience method to grow the buffer more space is required.
     */
    private fun growIfNeeded(required: Int) {
        if (buffer.remaining() < required) {
            val newBuffer = ByteBuffer.allocate((buffer.capacity() * growthFactor).toInt())
            buffer.flip()
            newBuffer.put(buffer)
            buffer = newBuffer
        }
    }
}

/**
 * Represents a view into multiple posting lists so that they can be iterated
 * easily.
 *
 * This class allows to have multiple posting lists stored in multiple shards,
 * and iterate over the postings as if they were a single posting list.
 *
 * This is used by [QueryResolver] objects to generate search results from
 * multiple shards.
 */
class PostingListView(private val buffers: List<ByteBuffer>) {

    fun peekableIterator(): PostingViewIterator {
        return PostingViewIterator(readPostings().iterator())
    }

    /**
     * Convenience implementation of an iterator where you can peek
     * the current element. As opposed to a regular iterator which only
     * allows to get the next element.
     *
     * This is convenient for merging multiple search results.
     */
    class PostingViewIterator(private val iterator: Iterator<Posting>) {
        private var current: Posting?

        init {
            if (iterator.hasNext()) {
                current = iterator.next()
            } else {
                current = null
            }
        }

        fun hasNext(): Boolean {
            return current != null
        }

        fun next(): Posting {
            val old = current
            if (iterator.hasNext()) {
                current = iterator.next()
            } else {
                current = null
            }
            return old ?: throw NoSuchElementException("Iterator doesn't have next")
        }

        fun peek(): Posting {
            return current ?: throw NoSuchElementException("Iterator doesn't have current")
        }
    }

    /**
     * Returns a sequence of [Posting] objects for this posting list.
     */
    fun readPostings(): Sequence<Posting> = sequence {
        for (buffer in buffers) {
            while (buffer.hasRemaining()) {
                val docId = buffer.getVarInt()
                val numPositions = buffer.getVarInt()
                var prev = 0
                for (j in 0 until numPositions) {
                    val position = buffer.getVarInt() + prev
                    yield(Posting(docId, position))
                    prev = position
                }
            }
        }
    }
}