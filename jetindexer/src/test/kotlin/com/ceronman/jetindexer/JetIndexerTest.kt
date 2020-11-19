// Copyright Manuel Cerón. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadLocalRandom
import kotlin.test.assertEquals

internal class JetIndexerTest {
    private lateinit var indexer: JetIndexer
    private lateinit var indexerJob: Job
    private val eventQueue = LinkedBlockingDeque<WatchEvent>()

    @BeforeEach
    internal fun setUp() {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    }

    @AfterEach
    internal fun tearDown() {
        indexer.stopWatching()
        runBlocking { indexerJob.join() }
    }

    @Test
    internal fun singleFileSearch(@TempDir tempDir: Path) {
        val path = writeFile(
            tempDir, """
            one two three
            four five six
            six seven eight
        """.trimIndent()
        )

        initIndexer(listOf(tempDir.toRealPath()))

        assertQuery(
            listOf(
                QueryResult("six", path, 24),
                QueryResult("six", path, 28)
            ),
            indexer.query("six").toList()
        )

        indexer.stopWatching()
    }

    @Test
    internal fun multipleFilesSearch(@TempDir tempDir: Path) {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")
        val path3 = writeFile(tempDir, "foo")

        initIndexer(listOf(tempDir.toRealPath()))

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three").toList()
        )

        assertQuery(
            listOf(QueryResult("foo", path3, 0)),
            indexer.query("foo").toList()
        )

        indexer.stopWatching()
    }

    @Test
    internal fun readError(@TempDir tempDir: Path) {
        val path = Paths.get("does", "not", "exit")
        initIndexer(listOf(path))
        assertQuery(
            emptyList(),
            indexer.query("test").toList()
        )
        indexer.stopWatching()
    }

    @Test
    internal fun goodFileAndBadFile(@TempDir tempDir: Path) {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = Paths.get("does", "not", "exit")

        initIndexer(listOf(tempDir.toRealPath(), path2))

        assertQuery(
            listOf(QueryResult("one", path1, 0)),
            indexer.query("one").toList()
        )
        indexer.stopWatching()
    }

    @Test
    internal fun deletedFile(@TempDir tempDir: Path) {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")

        initIndexer(listOf(tempDir.toRealPath()))

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three").toList()
        )

        Files.delete(path2)

        waitFor(WatchEvent.INDEX_UPDATED)

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
            ),
            indexer.query("three").toList()
        )

        indexer.stopWatching()
    }

    @Test
    internal fun updatedFile(@TempDir tempDir: Path) {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")

        initIndexer(listOf(tempDir.toRealPath()))

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three").toList()
        )

        assertQuery(
            listOf(
                QueryResult("five", path2, 11),
            ),
            indexer.query("five").toList()
        )

        path2.toFile().writeText("xxx yyyy three")

        waitFor(WatchEvent.INDEX_UPDATED)

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 9)
            ),
            indexer.query("three").toList()
        )

        assertQuery(
            emptyList<QueryResult>(),
            indexer.query("five").toList()
        )

        indexer.stopWatching()
    }

    @Test
    internal fun addedFile(@TempDir tempDir: Path) {
        val path1 = writeFile(tempDir, "one two three")
        val path2 = writeFile(tempDir, "three four five")

        initIndexer(listOf(tempDir.toRealPath()))

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three").toList()
        )

        assertQuery(
            listOf(
                QueryResult("five", path2, 11),
            ),
            indexer.query("five").toList()
        )

        val path3 = writeFile(tempDir, "xxx yyy zzz three")

        waitFor(WatchEvent.INDEX_UPDATED)

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0),
                QueryResult("three", path3, 12),
            ),
            indexer.query("three").toList()
        )

        assertQuery(
            listOf(
                QueryResult("five", path2, 11),
            ),
            indexer.query("five").toList()
        )
    }

    @Test
    internal fun nestedDirectories(@TempDir tempDir: Path) {
        val parent1 = Files.createDirectories(tempDir.resolve(Paths.get("a", "b", "c")))
        val parent2 = Files.createDirectories(tempDir.resolve(Paths.get("x", "y", "z")))
        val parent3 = Files.createDirectories(tempDir.resolve(Paths.get("m", "n", "o")))
        val path1 = writeFile(parent1, "one two three")
        val path2 = writeFile(parent2, "three four five")
        val path3 = writeFile(parent3, "foo")

        initIndexer(listOf(tempDir.toRealPath()))

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three").toList()
        )

        assertQuery(
            listOf(QueryResult("foo", path3, 0)),
            indexer.query("foo").toList()
        )

        indexer.stopWatching()
    }

    @Test
    internal fun symlinks(@TempDir tempDir: Path) {
        val parent1 = Files.createDirectories(tempDir.resolve(Paths.get("a", "b", "c")))
        val parent2 = Files.createDirectories(tempDir.resolve(Paths.get("x", "y", "z")))
        val parent3 = Files.createDirectories(tempDir.resolve(Paths.get("m", "n", "o")))
        val path1 = writeFile(parent1, "one two three")
        val path2 = writeFile(parent2, "three four five")
        val path3 = writeFile(parent3, "foo")
        val symlink = Files.createSymbolicLink(path3.parent.resolve("link.txt"), path3)

        initIndexer(listOf(tempDir.toRealPath()))

        assertQuery(
            listOf(
                QueryResult("three", path1, 8),
                QueryResult("three", path2, 0)
            ),
            indexer.query("three").toList()
        )

        assertQuery(
            listOf(
                QueryResult("foo", path3, 0),
                QueryResult("foo", symlink, 0)
            ),
            indexer.query("foo").toList()
        )

        indexer.stopWatching()
    }

    @Test
    internal fun concurrentSearches(@TempDir tempDir: Path) {
        fun randomWord(size: Int): String {
            val builder = StringBuilder()
            for (i in 0..size) {
                val char = ThreadLocalRandom.current().nextInt('a'.toInt(), 'z'.toInt() + 1)
                builder.appendCodePoint(char)
            }
            return builder.toString()
        }

        fun randomText(words: List<String>, size: Int): String {
            return (0..size).joinToString(" ") {
                words[ThreadLocalRandom.current().nextInt(0, words.size)]
            }
        }

        val words = (0..10000).map { randomWord(5) }

        for (i in 0..100) {
            writeFile(tempDir, randomText(words, 20))
        }

        initIndexer(listOf(tempDir.toRealPath()))
        val timeout = 1000L

        runBlocking {
            launch(Dispatchers.IO) {
                withTimeout(timeout) {
                    while (isActive) {
                        writeFile(tempDir, randomText(words, 20))
                        delay(100)
                    }

                }
            }
            for (i in 0..10) {
                launch(Dispatchers.IO) {
                    withTimeout(timeout) {
                        var empty = 0
                        var count = 0
                        while (isActive) {
                            val word = words[ThreadLocalRandom.current().nextInt(0, words.size)]
                            val result = indexer.query(word).toList()
                            if (result.isEmpty()) {
                                empty++
                            }
                            count++
                        }
                        val perSecond = (count / (timeout / 1000))
                        println("Made $count queries, $empty empty. $perSecond per second")
                    }
                }
            }
        }
    }

    private fun initIndexer(paths: List<Path>) {
        indexer = JetIndexer(paths)
        indexer.index()

        indexerJob = GlobalScope.launch(Dispatchers.Default) {
            indexer.watch {
                eventQueue.offer(it)
            }
        }
        waitFor(WatchEvent.WATCHER_CREATED)
    }

    private fun waitFor(e: WatchEvent) {
        while (eventQueue.take() != e) {
            Thread.sleep(10)
        }
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir.toRealPath(), "test", ".txt")
        path.toFile().writeText(contents)
        return path
    }

    private fun assertQuery(expected: List<QueryResult>, result: List<QueryResult>) {
        assertEquals(expected.sortedBy { it.path }, result.sortedBy { it.path })
    }
}