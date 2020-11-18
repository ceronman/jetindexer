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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

internal class VarIntTest {

    private val buffer: ByteBuffer = ByteBuffer.allocate(1024)

    @Test
    internal fun putSmallVarInt() {
        buffer.putVarInt(1)
        assertEquals(1, buffer.position())
        buffer.flip()
        assertEquals(1, buffer.getVarInt())
        buffer.clear()

        buffer.putVarInt(127)
        assertEquals(1, buffer.position())
        buffer.flip()
        assertEquals(127, buffer.getVarInt())
    }

    @Test
    internal fun putBigVarInt() {
        buffer.putVarInt(1024)
        assertEquals(2, buffer.position())
        buffer.flip()
        assertEquals(1024, buffer.getVarInt())

        buffer.clear()
        buffer.putVarInt(16_000)
        assertEquals(2, buffer.position())
        buffer.flip()
        assertEquals(16_000, buffer.getVarInt())
    }

    @Test
    internal fun putHugeVarInt() {
        buffer.putVarInt(Int.MAX_VALUE)
        assertEquals(5, buffer.position())
        buffer.flip()
        assertEquals(Int.MAX_VALUE, buffer.getVarInt())
    }

    @Test
    internal fun putNegativeVarInt() {
        buffer.putVarInt(-1)
        assertEquals(5, buffer.position())
        buffer.flip()
        assertEquals(-1, buffer.getVarInt())
    }
}