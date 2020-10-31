import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

internal class JetIndexerTest {
    @TempDir
    @JvmField
    var tempDirField: Path? = null
    private val tempDir: Path get() = tempDirField!!

    @BeforeEach
    internal fun setUp() {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    }

    @Test
    internal fun singleFileSearch() {
        val path = writeFile(
            tempDir, """
            one two three
            four five six
            six seven eight
        """.trimIndent()
        )

        val indexer = createIndexer()

        assertEquals(
            listOf(
                QueryResult("six", path, 24),
                QueryResult("six", path, 28)
            ).sortedBy { it.path },
            indexer.query("six").sortedBy { it.path })
    }

    @Test
    internal fun multipleFilesSearch() {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")
        val path3 = writeFile(tempDir, "foo")

        val indexer = createIndexer()

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

    @Test
    internal fun readError() {
        val path = Paths.get("does", "not", "exit")
        val indexer = createIndexer()
        assertEquals(
            emptyList<QueryResult>(),
            indexer.query("test")
        )
    }

    @Test
    internal fun goodFileAndBadFile() {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = Paths.get("does", "not", "exit")
        val indexer = createIndexer()
        assertEquals(
            listOf(QueryResult("one", path1, 0)),
            indexer.query("one")
        )
    }

    @Test
    internal fun deletedFile() {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")

        val indexer = createIndexer()

        assertEquals(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ).sortedBy { it.path },
            indexer.query("three").sortedBy { it.path })

        indexer.deleteDocument(path2)

        assertEquals(
            listOf(
                QueryResult("three", path1, 8),
            ).sortedBy { it.path },
            indexer.query("three").sortedBy { it.path })
    }

    @Test
    internal fun updatedFile() {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")

        val indexer = createIndexer()

        assertEquals(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ).sortedBy { it.path },
            indexer.query("three").sortedBy { it.path })

        assertEquals(
            listOf(
                QueryResult("five", path2, 11),
            ).sortedBy { it.path },
            indexer.query("five").sortedBy { it.path })

        path2.toFile().writeText("xxx yyyy three")
        indexer.updateDocument(path2)

        assertEquals(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 9)
            ).sortedBy { it.path },
            indexer.query("three").sortedBy { it.path })

        assertEquals(
            emptyList<QueryResult>(),
            indexer.query("five")
        )
    }

    @Test
    internal fun nestedDirectories() {
        val parent1 = Files.createDirectories(tempDir.resolve(Paths.get("a", "b", "c")))
        val parent2 = Files.createDirectories(tempDir.resolve(Paths.get("x", "y", "z")))
        val parent3 = Files.createDirectories(tempDir.resolve(Paths.get("m", "n", "o")))
        val path1 = writeFile(parent1, "one two three")
        val path2 = writeFile(parent2, "three four five")
        val path3 = writeFile(parent3, "foo")

        val indexer = createIndexer()

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

    @Test
    internal fun symlinks(@TempDir tempDir: Path) {
        val parent1 = Files.createDirectories(tempDir.resolve(Paths.get("a", "b", "c")))
        val parent2 = Files.createDirectories(tempDir.resolve(Paths.get("x", "y", "z")))
        val parent3 = Files.createDirectories(tempDir.resolve(Paths.get("m", "n", "o")))
        val path1 = writeFile(parent1, "one two three")
        val path2 = writeFile(parent2, "three four five")
        val path3 = writeFile(parent3, "foo")
        val symlink = Files.createSymbolicLink(tempDir.resolve("link"), path3)

        val indexer = createIndexer()

        assertEquals(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ).sortedBy { it.path },
            indexer.query("three").sortedBy { it.path })

        assertEquals(
            listOf(
                QueryResult("foo", path3, 0),
                QueryResult("foo", symlink, 0)
            ),
            indexer.query("foo")
        )
    }

    private fun createIndexer(): JetIndexer {
        val indexer = JetIndexer(WhiteSpaceTokenizer(), listOf(tempDir))

        GlobalScope.launch {
            indexer.index()
        }
        runBlocking {
            for (p in indexer.indexingProgress) {
                println("Loading ${p * 100.0}%")
            }
        }
        return indexer
    }

    private fun writeFile(dir: Path?, contents: String): Path {
        val path = Files.createTempFile(dir, "test", "test")
        path.toFile().writeText(contents)
        return path
    }
}

