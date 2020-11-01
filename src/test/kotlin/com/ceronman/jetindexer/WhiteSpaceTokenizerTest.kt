package com.ceronman.jetindexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class WhiteSpaceTokenizerTest {

    @Test
    internal fun simpleWords() {
        val tokenizer = WhiteSpaceTokenizer()
        assertEquals(
            listOf(
                Token("one", 0),
                Token("two", 4),
                Token("three", 8)
            ),
            tokenizer.tokenize("one two three".asSequence()).toList()
        )
    }

    @Test
    internal fun multipleSpaces() {
        val tokenizer = WhiteSpaceTokenizer()
        assertEquals(
            listOf(
                Token("one", 0),
                Token("two", 10),
                Token("three", 14)
            ),
            tokenizer.tokenize("one       two three".asSequence()).toList()
        )
    }

    @Test
    internal fun differentWhitespaceChars() {
        val tokenizer = WhiteSpaceTokenizer()
        assertEquals(
            listOf(
                Token("one", 0),
                Token("two", 4),
                Token("three", 8)
            ),
            tokenizer.tokenize("one\ttwo\nthree".asSequence()).toList()
        )
    }
}