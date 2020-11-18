package com.ceronman.jetindexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

internal class TrigramTokenizerTest {
    @Test
    internal fun simpleWordsString() {
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
            tokenizer.tokenize("one two three").toList()
        )
    }

    @Test
    internal fun simpleWordsFromFile(@TempDir tempDir: Path) {
        val tokenizer = TrigramTokenizer()
        val path = writeFile(tempDir, "one two three")
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
            tokenizer.tokenize(path).toList()
        )
    }

    @Test
    internal fun smallWord(@TempDir tempDir: Path) {
        val tokenizer = TrigramTokenizer()
        val path = writeFile(tempDir, "on")
        assertEquals(
            emptyList<Token>(),
            tokenizer.tokenize(path).toList()
        )
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir, "test", "test")
        path.toFile().writeText(contents)
        return path
    }
}