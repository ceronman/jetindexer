package com.ceronman.jetindexer

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

internal class TrigramSubstringQueryResolverTest {

    @Test
    fun search(@TempDir tempDir: Path) {
        val index = TokenIndex(TrigramTokenizer())
        val path1 = writeFile(tempDir, "six sixteen sixty")
        val path2 = writeFile(tempDir, "-----six----")
        val path3 = writeFile(tempDir, "And sssssix")
        val path4 = writeFile(tempDir, "Other text")

        index.addBatch(listOf(path1, path2, path3, path4))

        val results3 = TrigramSubstringQueryResolver().search(index, "six")

        kotlin.test.assertEquals(
            listOf(
                QueryResult("six", path1, 0),
                QueryResult("six", path1, 4),
                QueryResult("six", path1, 12),
                QueryResult("six", path2, 5),
                QueryResult("six", path3, 8),
            ),
            results3
        )
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir.toRealPath(), "test", ".txt")
        path.toFile().writeText(contents)
        return path
    }
}