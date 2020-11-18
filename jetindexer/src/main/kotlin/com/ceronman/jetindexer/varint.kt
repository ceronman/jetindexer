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

const val VAR_INT_MAX_SIZE = 5

/**
 * Puts an integer into a [ByteBuffer] encoded using variable byte representation
 *
 * https://nlp.stanford.edu/IR-book/html/htmledition/variable-byte-codes-1.html
 */
fun ByteBuffer.putVarInt(value: Int): ByteBuffer {
    var v = value
    while (true) {
        val bits = v and 0x7f
        v = v ushr 7
        if (v == 0) {
            this.put(bits.toByte())
            return this
        }
        this.put((bits or 0x80).toByte())
    }
}

/**
 * Reads an integer from a [ByteBuffer] encoded using variable byte representation
 *
 * https://nlp.stanford.edu/IR-book/html/htmledition/variable-byte-codes-1.html
 */
fun ByteBuffer.getVarInt(): Int {
    var tmp = this.get().toInt()
    if (tmp >= 0) {
        return tmp
    }
    var result = tmp and 0x7f
    tmp = this.get().toInt()
    if (tmp >= 0) {
        result = result or (tmp shl 7)
    } else {
        result = result or (tmp and 0x7f shl 7)
        tmp = this.get().toInt()
        if (tmp >= 0) {
            result = result or (tmp shl 14)
        } else {
            result = result or (tmp and 0x7f shl 14)
            tmp = this.get().toInt()
            if (tmp >= 0) {
                result = result or (tmp shl 21)
            } else {
                result = result or (tmp and 0x7f shl 21)
                tmp = this.get().toInt()
                result = result or (tmp shl 28)
            }
        }
    }
    return result
}