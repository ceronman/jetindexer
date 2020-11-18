package com.ceronman.jetindexer

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class TokenIndexTest {

    @Test
    internal fun addBatch(@TempDir tempDir: Path) {
        val index = TokenIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "one two three four")
        val path2 = writeFile(tempDir, "two four six eight")
        val path3 = writeFile(tempDir, "two four eight sixteen")

        index.addBatch(listOf(path1, path2, path3)) { progress ->
            assertTrue { progress in 0..100 }
        }

        val results3 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 0),
            ),
            results3
        )
    }

    @Test
    internal fun add(@TempDir tempDir: Path) {
        val index = TokenIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "one two three four")
        val path2 = writeFile(tempDir, "two four six eight")
        val path3 = writeFile(tempDir, "two four eight sixteen")
        index.add(path1)

        val results1 = StandardQueryResolver().search(index, "one")

        assertEquals(
            listOf(QueryResult("one", path1, 0)),
            results1
        )

        index.add(path2)
        val results2 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
            ),
            results2
        )

        index.add(path3)
        val results3 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 0),
            ),
            results3
        )
    }

    @Test
    internal fun delete(@TempDir tempDir: Path) {
        val index = TokenIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "one two three four")
        val path2 = writeFile(tempDir, "two four six eight")
        val path3 = writeFile(tempDir, "two four eight sixteen")
        index.add(path1)
        index.add(path2)
        index.add(path3)

        val results1 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 0),
            ),
            results1
        )

        index.delete(path3)

        val results2 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
            ),
            results2
        )
    }

    @Test
    internal fun update(@TempDir tempDir: Path) {
        val index = TokenIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "one two three four")
        val path2 = writeFile(tempDir, "two four six eight")
        val path3 = writeFile(tempDir, "two four eight sixteen")
        index.add(path1)
        index.add(path2)
        index.add(path3)

        val results1 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 0),
            ),
            results1
        )

        Files.writeString(path3, "one two")
        index.update(path3)

        val results2 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 4),
            ),
            results2
        )
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir.toRealPath(), "test", ".txt")
        path.toFile().writeText(contents)
        return path
    }
}