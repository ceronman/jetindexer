package com.ceronman.jetindexer

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.takeWhile
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertEquals

internal class JetIndexerTest {
    @TempDir
    @JvmField
    var tempDirField: Path? = null
    private val tempDir: Path get() = tempDirField!!

    private lateinit var indexer: JetIndexer
    private lateinit var indexerJob: Job

    @BeforeEach
    internal fun setUp() {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    }

    @AfterEach
    internal fun tearDown() {
        indexer.stop()
        runBlocking { indexerJob.join() }
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

        initIndexer()

        assertQuery(
            listOf(
                QueryResult("six", path, 24),
                QueryResult("six", path, 28)
            ),
            indexer.query("six"))

        indexer.stop()
    }

    @Test
    internal fun multipleFilesSearch() {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")
        val path3 = writeFile(tempDir, "foo")

        initIndexer()

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three"))

        assertQuery(
            listOf(QueryResult("foo", path3, 0)),
            indexer.query("foo")
        )

        indexer.stop()
    }

    @Test
    internal fun readError() {
        val path = Paths.get("does", "not", "exit")
        initIndexer(listOf(path))
        assertQuery(
            emptyList(),
            indexer.query("test")
        )
        indexer.stop()
    }

    @Test
    internal fun goodFileAndBadFile() {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = Paths.get("does", "not", "exit")

        initIndexer(listOf(tempDir, path2))

        assertQuery(
            listOf(QueryResult("one", path1, 0)),
            indexer.query("one")
        )
        indexer.stop()
    }

    @Test
    internal fun deletedFile() {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")

        initIndexer()

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three"))

        Files.delete(path2)

        Thread.sleep(100) // TODO: Improve this by using an event channel

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
            ),
            indexer.query("three"))

        indexer.stop()
    }

    @Test
    internal fun updatedFile() {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")

        initIndexer()

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three"))

        assertQuery(
            listOf(
                QueryResult("five", path2, 11),
            ),
            indexer.query("five"))

        path2.toFile().writeText("xxx yyyy three")

        Thread.sleep(100) // TODO: Improve this by using an event channel

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 9)
            ),
            indexer.query("three"))

        assertQuery(
            emptyList<QueryResult>(),
            indexer.query("five")
        )

        indexer.stop()
    }

    @Test
    internal fun addedFile() = runBlocking {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")

        initIndexer()

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three")
        )

        assertQuery(
            listOf(
                QueryResult("five", path2, 11),
            ),
            indexer.query("five")
        )

        val path3 = writeFile(tempDir, "xxx yyy zzz three")

        delay(100) // TODO: Improve this by using an event

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0),
                QueryResult("three", path3, 12),
            ),
            indexer.query("three")
        )

        assertQuery(
            listOf(
                QueryResult("five", path2, 11),
            ),
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

        initIndexer()

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three"))

        assertQuery(
            listOf(QueryResult("foo", path3, 0)),
            indexer.query("foo")
        )

        indexer.stop()
    }

    @Test
    internal fun symlinks() {
        val parent1 = Files.createDirectories(tempDir.resolve(Paths.get("a", "b", "c")))
        val parent2 = Files.createDirectories(tempDir.resolve(Paths.get("x", "y", "z")))
        val parent3 = Files.createDirectories(tempDir.resolve(Paths.get("m", "n", "o")))
        val path1 = writeFile(parent1, "one two three")
        val path2 = writeFile(parent2, "three four five")
        val path3 = writeFile(parent3, "foo")
        val symlink = Files.createSymbolicLink(tempDir.resolve("link"), path3)

        initIndexer()

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three"))

        assertQuery(
            listOf(
                QueryResult("foo", path3, 0),
                QueryResult("foo", symlink, 0)
            ),
            indexer.query("foo")
        )

        indexer.stop()
    }

    private fun initIndexer(paths: List<Path> = listOf(tempDir)) {
        indexer = JetIndexer(WhiteSpaceTokenizer(), paths)

        indexerJob = GlobalScope.launch {
            indexer.index()
        }

        runBlocking {
            indexer.events.filter { it is IndexingProgressEvent && it.done }
                .take(1)
                .collect { }
        }
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir, "test", "test")
        path.toFile().writeText(contents)
        return path
    }

    private fun assertQuery(expected: List<QueryResult>, result: List<QueryResult>) {
        assertEquals(expected.sortedBy { it.path }, result.sortedBy { it.path })
    }
}