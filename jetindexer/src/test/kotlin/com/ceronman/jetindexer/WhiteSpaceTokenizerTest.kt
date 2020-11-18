package com.ceronman.jetindexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

internal class WhiteSpaceTokenizerTest {

    @Test
    internal fun simpleWords(@TempDir tempDir: Path) {
        val tokenizer = WhiteSpaceTokenizer()
        val path = writeFile(tempDir, "one two three")
        assertEquals(
            listOf(
                Token("one", 0),
                Token("two", 4),
                Token("three", 8)
            ),
            tokenizer.tokenize(path).toList()
        )
    }

    @Test
    internal fun multipleSpaces(@TempDir tempDir: Path) {
        val tokenizer = WhiteSpaceTokenizer()
        val path = writeFile(tempDir, "one       two three")
        assertEquals(
            listOf(
                Token("one", 0),
                Token("two", 10),
                Token("three", 14)
            ),
            tokenizer.tokenize(path).toList()
        )
    }

    @Test
    internal fun differentWhitespaceChars(@TempDir tempDir: Path) {
        val tokenizer = WhiteSpaceTokenizer()
        val path = writeFile(tempDir, "one\ttwo\nthree")
        assertEquals(
            listOf(
                Token("one", 0),
                Token("two", 4),
                Token("three", 8)
            ),
            tokenizer.tokenize(path).toList()
        )
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir, "test", "test")
        path.toFile().writeText(contents)
        return path
    }
}