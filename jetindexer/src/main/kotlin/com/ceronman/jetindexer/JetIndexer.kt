package com.ceronman.jetindexer

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.LoggerFactory
import java.nio.file.ClosedWatchServiceException
import java.nio.file.Files
import java.nio.file.Path
import kotlin.system.measureTimeMillis

class JetIndexer(
    tokenizer: Tokenizer,
    private val queryResolver: QueryResolver,
    indexingFilter: IndexingFilter,
    private val paths: Collection<Path>
) {
    private lateinit var directoryPaths: List<Path>
    private val log = LoggerFactory.getLogger(this.javaClass)
    private val index = TokenIndex(tokenizer)
    private val fileWalker = FileWalker(indexingFilter)
    private lateinit var watcher: DirectoryWatcher

    fun indexAndWatch(progressCallback: ProgressCallback, watchEventCallback: WatchEventCallback) {
        index(progressCallback)
        watch(watchEventCallback)
    }

    fun index(progressCallback: ProgressCallback = null) {
        progressCallback?.invoke(1)
        directoryPaths = filterPaths(paths)
        progressCallback?.invoke(2)
        val filePaths = fileWalker.walk(directoryPaths)
        val t = measureTimeMillis {
            index.addBatch(filePaths, progressCallback)
        }
        log.debug("Indexing paths took ${t.toDouble() / 1000} seconds")

        log.debug("FileWatcher is ready")
    }

    fun watch(eventCallback: WatchEventCallback) {
        watcher = DirectoryWatcher.builder()
            .paths(directoryPaths)
            .listener(object : DirectoryChangeListener {
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
            })
            .build()
        eventCallback?.invoke(WatchEvent.WATCHER_CREATED)
        log.info("Starting file watcher")
        try {
            watcher.watch()
        } catch (e: ClosedWatchServiceException) {
            log.info("File watcher was closed before starting to watch")
        }

        log.info("Terminating watch")
    }

    fun stop() {
        log.info("Stopping watch service")
        watcher.close()
    }

    fun query(term: String): List<QueryResult> {
        return queryResolver.search(index, term)
    }

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
}

enum class WatchEvent {
    WATCHER_CREATED,
    INDEX_UPDATED
}

typealias WatchEventCallback = ((WatchEvent) -> Unit)?
