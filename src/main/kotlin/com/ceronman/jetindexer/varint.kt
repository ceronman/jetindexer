package com.ceronman.jetindexer

import java.nio.ByteBuffer

const val VAR_INT_MAX_SIZE = 5

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
    return this
}

fun ByteBuffer.getVarInt(): Int {
    var tmp = this.get().toInt()
    if (tmp  >= 0) {
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