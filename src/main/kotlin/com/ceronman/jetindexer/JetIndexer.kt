package com.ceronman.jetindexer

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import org.slf4j.LoggerFactory
import java.nio.file.*
import kotlin.collections.ArrayList
import kotlin.system.measureTimeMillis


class JetIndexer(
        tokenizer: Tokenizer,
        private val queryResolver: QueryResolver,
        indexingFilter: IndexingFilter,
        private val paths: Collection<Path>
) {
    val events: Flow<Event> get() = _events.asSharedFlow()
    private val _events = MutableSharedFlow<Event>()
    private val log = LoggerFactory.getLogger(this.javaClass)
    private val index = TokenIndex(tokenizer)
    private val fileWalker = FileWalker(indexingFilter)
    @Volatile private var watcher: DirectoryWatcher? = null

    suspend fun index() = withContext(Dispatchers.Default) {
        val directoryPaths = filterPaths(paths)
        val filePaths = fileWalker.walk(paths)

        val t = measureTimeMillis {
            index.addBatch(filePaths)
        }
        log.info("Indexed ${filePaths.size} documents in ${t.toDouble() / 1000.0} seconds")
        log.debug("Prepared to watch $directoryPaths")

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
                            }
                        }
                        DirectoryChangeEvent.EventType.DELETE -> {
                            log.info("Watcher delete event ${event.path()}")
                            index.delete(event.path())
                        }
                        DirectoryChangeEvent.EventType.MODIFY -> {
                            log.info("Watcher modify event ${event.path()}")
                            if (Files.isRegularFile(event.path())) {
                                index.update(event.path())
                            }
                        }
                        DirectoryChangeEvent.EventType.OVERFLOW -> {
                            log.warn("File watcher overflowed!")
                        }
                    }

                    // TODO: This doesn't seem right!
                    runBlocking { _events.emit(IndexUpdateEvent()) }
                }
            })
            .build()
        log.debug("Watcher was built")

        _events.emit(IndexingProgressEvent(100, true))

        try {
            watcher!!.watch()
        } catch (e: ClosedWatchServiceException) {
            log.warn("Closed service")
        }
        log.info("Terminating watch")
    }

    fun stop() {
        log.info("Stopping watch service")
        if (watcher == null) {
            log.error("Watcher is null!")
        }
        watcher?.close()
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

sealed class Event
class IndexUpdateEvent(): Event()
data class IndexingProgressEvent(val progress: Int, val done: Boolean): Event()
