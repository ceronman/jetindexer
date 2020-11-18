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