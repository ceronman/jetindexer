package com.ceronman.jetindexer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

internal class RegexTokenizerTest {
    @Test
    internal fun differentWhitespaceChars(@TempDir tempDir: Path) {
        val tokenizer = RegexTokenizer("[_0-9a-zA-Z]+".toRegex())
        val path = writeFile(tempDir, "one;two |three.four(six)[seven]{eight}")
        assertEquals(
                listOf(
                    Token("one", 0),
                    Token("two", 4),
                    Token("three", 9),
                    Token("four", 15),
                    Token("six", 20),
                    Token("seven", 25),
                    Token("eight", 32),
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