package com.ceronman.jetindexer

import java.nio.ByteBuffer

class DeltaVarInt {
    private var buffer = ByteBuffer.allocate(1024)

    fun encode(values: List<Int>): ByteArray {
        buffer.putInt(values.size)
        if (values.isEmpty()) {
            return toByteArray()
        }
        var prev = values[0]
        putVarInt(prev)
        for (i in 1 until values.size) {
            putVarInt(values[i] - prev)
            prev = values[i]
        }
        return toByteArray()
    }

    private fun grow() {
        val newBuffer = ByteBuffer.allocate(buffer.capacity() * 2)
        buffer.flip()
        newBuffer.put(buffer)
        buffer = newBuffer
    }

    private fun putVarInt(value: Int) {
        var v = value
        while (true) {
            val bits = v and 0x7f
            v = v ushr 7

            if (!buffer.hasRemaining()) {
                grow()
            }

            if (v == 0) {
                buffer.put(bits.toByte())
                return
            }
            buffer.put((bits or 0x80).toByte())
        }
    }

    private fun toByteArray(): ByteArray {
        buffer.flip()
        val result = ByteArray(buffer.remaining())
        buffer.get(result)
        buffer.clear()
        return result
    }

    companion object {
        fun decode(encodedValues: ByteArray): List<Int> {
            val buffer = ByteBuffer.wrap(encodedValues)
            val size = buffer.getInt()
            val result = ArrayList<Int>(size)
            var prev = 0
            for (i in 0 until size) {
                val value = prev + getVarInt(buffer)
                result.add(value)
                prev = value
            }
            return result
        }

        private fun getVarInt(buffer: ByteBuffer): Int {
            var tmp = buffer.get().toInt()
            if (tmp  >= 0) {
                return tmp
            }
            var result = tmp and 0x7f
            tmp = buffer.get().toInt()
            if (tmp >= 0) {
                result = result or (tmp shl 7)
            } else {
                result = result or (tmp and 0x7f shl 7)
                tmp = buffer.get().toInt()
                if (tmp >= 0) {
                    result = result or (tmp shl 14)
                } else {
                    result = result or (tmp and 0x7f shl 14)
                    tmp = buffer.get().toInt()
                    if (tmp >= 0) {
                        result = result or (tmp shl 21)
                    } else {
                        result = result or (tmp and 0x7f shl 21)
                        tmp = buffer.get().toInt()
                        result = result or (tmp shl 28)
                    }
                }
            }
            return result
        }
    }
}
