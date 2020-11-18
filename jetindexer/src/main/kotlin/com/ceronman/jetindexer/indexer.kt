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

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.LoggerFactory
import java.nio.file.ClosedWatchServiceException
import java.nio.file.Files
import java.nio.file.Path
import kotlin.system.measureTimeMillis

/**
 * JetIndexer is a text search engine library.
 *
 * JetIndexer takes a list of directories and it indexes text files contained in those
 * recursively. Additionally, it can watch those directories for changes and update
 * the index accordingly. Once the index is built it is possible to search text
 * in the indexed files in a fast way.
 *
 * The indexation process can be customized by providing a custom [Tokenizer] and or
 * [QueryResolver]. Filtering which files should be indexed can be configured by
 * providing a [IndexingFilter] object.
 *
 * Example usage:
 *
 * ```
 * val path = Paths.get("/path/to/files")
 * val indexer = JetIndexer(listOf(path))
 * indexer.index()
 * indexer.query("foo")
 * ```
 *
 * @param paths A collection of [Path] objects containing the documents to index.
 * @param tokenizer A [Tokenizer] object to split the documents in tokens to index.
 *  By default it uses a [WhiteSpaceTokenizer]
 * @param queryResolver A [QueryResolver] for advanced search capabilities. By
 *  default it uses a [StandardQueryResolver]
 * @param indexingFilter A [IndexingFilter] object to restrict what kinds of files
 *  should be indexes and which ones shouldn't. By default it uses a [DefaultIndexingFilter]
 */
class JetIndexer(
    private val paths: Collection<Path>,
    tokenizer: Tokenizer = WhiteSpaceTokenizer(),
    private val queryResolver: QueryResolver = StandardQueryResolver(),
    indexingFilter: IndexingFilter = DefaultIndexingFilter(),
) {
    private val log = LoggerFactory.getLogger(this.javaClass)
    private val index = InvertedIndex(tokenizer)
    private val fileWalker = FileWalker(indexingFilter)
    private lateinit var directoryPaths: List<Path>
    private lateinit var watcher: DirectoryWatcher

    /**
     * Starts the indexing process by recursively traversing the provided paths and
     * tokenizing each text file found and accepted by the [IndexingFilter]
     *
     * @param progressCallback: A callback function accepting an integer which represents
     *  the progress of the indexing process. The number goes from 1 to 100.
     */
    fun index(progressCallback: ProgressCallback = null) {
        progressCallback?.invoke(1)
        directoryPaths = filterPaths(paths)
        progressCallback?.invoke(2)
        val filePaths = fileWalker.walk(directoryPaths)
        val t = measureTimeMillis {
            index.addBatch(filePaths, progressCallback)
        }
        log.debug("Indexing paths took ${t.toDouble() / 1000} seconds")
    }

    /**
     * Starts the watching the provided paths recursively. For any file created, deleted
     * or modified, the index will be updated accordingly.
     *
     * This method blocks. It should run in a separate thread or coroutine.
     *
     * @param eventCallback: A callback function accepting a [WatchEvent] object that will
     * report when the watcher is ready and when a change has occurred in the index.
     */
    fun watch(eventCallback: WatchEventCallback) {
        watcher = DirectoryWatcher.builder()
            .paths(directoryPaths)
            .listener(WatcherListener(eventCallback))
            .build()

        log.debug("FileWatcher is ready")

        eventCallback?.invoke(WatchEvent.WATCHER_CREATED)

        log.info("Starting file watcher")
        try {
            watcher.watch()
        } catch (e: ClosedWatchServiceException) {
            log.warn("File watcher was closed before starting to watch")
        }

        log.info("Terminating watch")
    }

    /**
     * Starts the indexing process and immediately start the file watching process.
     * @see [index]
     * @see [watch]
     *
     * @param progressCallback A callback function accepting a [WatchEvent] object that will
     * report when the watcher is ready and when a change has occurred in the index.
     * @param watchEventCallback A callback function accepting a [WatchEvent] object that will
     * report when the watcher is ready and when a change has occurred in the index.
     *
     */
    fun indexAndWatch(progressCallback: ProgressCallback, watchEventCallback: WatchEventCallback) {
        index(progressCallback)
        watch(watchEventCallback)
    }

    /**
     * Stops the watching process.
     *
     * @throws [IllegalStateException] if the watching process has never started.
     */
    fun stopWatching() {
        if (!::watcher.isInitialized) {
            throw IllegalStateException("Watcher has not started yet.")
        }
        log.info("Stopping watch service")
        watcher.close()
    }

    /**
     * Search a given term in the index. The capabilities of the search are dictated by the
     * [QueryResolver] used.
     *
     * @return A list of [QueryResult] objects indicating the matched term, the file name matching
     * and the position in the file of the term since the beginning of the file.
     */
    fun query(term: String): List<QueryResult> {
        return queryResolver.search(index, term)
    }

    /**
     * A sanity check filter that removes incorrect paths such as non-existent directories or
     * regular files.
     *
     * @param paths Collection of [Path] objects to filter
     * @return A list of [Path] with all bad paths filtered out.
     */
    private fun filterPaths(paths: Collection<Path>): List<Path> {
        val filteredPaths = ArrayList<Path>()
        for (path in paths) {
            if (!Files.isDirectory(path)) {
                log.warn("Path $path is not a directory, individual files are not supported")
                continue
            }
            if (!Files.exists(path)) {
                log.warn("Path $path does not exit. Ignoring")
                continue
            }
            filteredPaths.add(path.toAbsolutePath())
        }
        return filteredPaths
    }

    /**
     * A [DirectoryChangeListener] implementation for watching for changes in the file system.
     * This is used for the file watcher which is based on:
     * https://github.com/gmethvin/directory-watcher
     */
    private inner class WatcherListener(private val eventCallback: WatchEventCallback) : DirectoryChangeListener {
        override fun onEvent(event: DirectoryChangeEvent?) {
            if (event == null) {
                return
            }
            when (event.eventType()) {
                DirectoryChangeEvent.EventType.CREATE -> {
                    log.info("Watcher create event ${event.path()}")
                    if (Files.isRegularFile(event.path())) {
                        index.add(event.path())
                        eventCallback?.invoke(WatchEvent.INDEX_UPDATED)
                    }
                }
                DirectoryChangeEvent.EventType.DELETE -> {
                    log.info("Watcher delete event ${event.path()}")
                    index.delete(event.path())
                    eventCallback?.invoke(WatchEvent.INDEX_UPDATED)
                }
                DirectoryChangeEvent.EventType.MODIFY -> {
                    log.info("Watcher modify event ${event.path()}")
                    if (Files.isRegularFile(event.path())) {
                        index.update(event.path())
                        eventCallback?.invoke(WatchEvent.INDEX_UPDATED)
                    }
                }
                DirectoryChangeEvent.EventType.OVERFLOW -> {
                    log.warn("File watcher overflowed!")
                }
                else -> {
                    log.error("An unknown error has been received from the file watcher")
                }
            }
        }
    }
}

/**
 * Represent an event that happening during the directory watched process.
 *
 * [WATCHER_CREATED] represents when the internal [DirectoryWatcher] object has been fully built.
 * [INDEX_UPDATED] represents a change in the file system that resulted in the index being updated.
 */
enum class WatchEvent {
    WATCHER_CREATED,
    INDEX_UPDATED
}

/**
 * The callback that is accepted by the file watching process and reporting [WatchEvent] objects
 * @see [JetIndexer.watch]
 */
typealias WatchEventCallback = ((WatchEvent) -> Unit)?
