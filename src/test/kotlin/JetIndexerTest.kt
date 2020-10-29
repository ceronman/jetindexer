import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

internal class JetIndexerTest {
    @Test
    internal fun singleFileSearch(@TempDir tempDir: Path) {
        val path = writeFile(
            tempDir, """
            one two three
            four five six
            six seven eight
        """.trimIndent()
        )

        val indexer = JetIndexer(WhiteSpaceTokenizer(), listOf(path))
        indexer.start()

        assertEquals(
            listOf(
                QueryResult("six", path, 24),
                QueryResult("six", path, 28)
            ).sortedBy { it.path },
            indexer.query("six").sortedBy { it.path })
    }

    @Test
    internal fun multipleFilesSearch(@TempDir tempDir: Path) {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")
        val path3 = writeFile(tempDir, "foo")

        val indexer = JetIndexer(WhiteSpaceTokenizer(), listOf(path1, path2, path3))
        indexer.start()

        assertEquals(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ).sortedBy { it.path },
            indexer.query("three").sortedBy { it.path })

        assertEquals(
            listOf(QueryResult("foo", path3, 0)),
            indexer.query("foo")
        )
    }
}

private fun writeFile(dir: Path, contents: String): Path {
    val path = Files.createTempFile(dir, "test", "test")
    path.toFile().writeText(contents)
    return path
}