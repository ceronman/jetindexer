package com.ceronman.jetindexer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class TrigramTokenizerTest {
    @Test
    internal fun simpleWords() {
        val tokenizer = TrigramTokenizer()
        assertEquals(
            listOf(
                Token("one", 0),
                Token("ne ", 1),
                Token("e t", 2),
                Token(" tw", 3),
                Token("two", 4),
                Token("wo ", 5),
                Token("o t", 6),
                Token(" th", 7),
                Token("thr", 8),
                Token("hre", 9),
                Token("ree", 10),
            ),
            tokenizer.tokenize("one two three".asSequence()).toList()
        )
    }
}