package com.ceronman.jetindexer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import kotlin.random.Random

internal class DeltaVarIntTest {
    @Test
    fun encode() {
        val values = listOf(1, 2, 3, 4, 5, 6, 7, 8)
        val encoder = DeltaVarInt()
        val result = encoder.encode(values)
        assertEquals(values, DeltaVarInt.decode(result))
    }

    @Test
    fun encode2() {
        val values = listOf(10, 20, 30, 60, 90, 200, 2015)
        val encoder = DeltaVarInt()
        val result = encoder.encode(values)
        assertEquals(values, DeltaVarInt.decode(result))
    }

    @Test
    fun random() {
        val encoder = DeltaVarInt()
        for (i in 0..1000) {
            val size = Random.nextInt(1000, 10000)
            val values = ArrayList<Int>(size)
            var prev = Random.nextInt(0, 20)
            for (j in 0..size) {
                val value = prev + Random.nextInt(0, 1000)
                values.add(value)
                prev = value
            }
            val result = encoder.encode(values)
            assertEquals(values, DeltaVarInt.decode(result))
        }
    }
}