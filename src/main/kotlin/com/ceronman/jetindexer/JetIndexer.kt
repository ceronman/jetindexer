package com.ceronman.jetindexer

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.collections.ArrayList
import kotlin.system.measureTimeMillis


class JetIndexer(
    private val tokenizer: Tokenizer,
    private val paths: Collection<Path>
) {
    val events: Flow<Event> get() = _events.asSharedFlow()
    private val _events = MutableSharedFlow<Event>()

    private val simpleIndex = ConcurrentHashMap<String, MutableList<Int>>()
    private val log = LoggerFactory.getLogger(this.javaClass)
    private val index = ConcurrentHashMap<String, MutableMap<Path, List<Int>>>()
    private val documents = ConcurrentHashMap<Path, Document>()
    @Volatile private var watcher: DirectoryWatcher? = null

    suspend fun index() = withContext(Dispatchers.Default) {
        val directoryPaths = filterPaths(paths)
        val filePaths = walkPaths(directoryPaths)
        filePaths.withIndex()
        val jobs = ArrayList<Job>()
        var lastPath: Path? = null
        for ((i, path) in filePaths.withIndex()) {
            if (path.toString().endsWith(".java")) {
                lastPath = path
            }
            val job = launch {
                val progress = (i.toFloat() / filePaths.size.toFloat()) * 100.0
                addDocument(path)
                _events.emit(IndexingProgressEvent(progress.toInt(), false))
            }
            jobs.add(job)
        }
        val indexingTime = measureTimeMillis {
            jobs.joinAll()
        }

        log.info("Indexed ${documents.size} documents in ${indexingTime.toDouble() / 1000.0} seconds")
        log.info("Index contains ${index.size} terms")

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
                            addDocument(event.path())
                        }
                        DirectoryChangeEvent.EventType.DELETE -> {
                            log.info("Watcher delete event ${event.path()}")
                            deleteDocument(event.path())
                        }
                        DirectoryChangeEvent.EventType.MODIFY -> {
                            log.info("Watcher modify event ${event.path()}")
                            updateDocument(event.path())
                        }
                        DirectoryChangeEvent.EventType.OVERFLOW -> {
                            log.info("Watcher overflow event")
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
        val matches = index[term] ?: return emptyList()
        val result = ArrayList<QueryResult>()
        for ((path, positions) in matches) {
            for (position in positions) {
                result.add(QueryResult(term, path, position))
            }
        }
        return result
    }

    private fun addDocument(path: Path) {
//        log.info("Adding $path (${index.size})")

        try {
            // TODO: test this
            val contentType = Files.probeContentType(path)
            if (!contentType.contains("text")) {
//                log.warn("File $path is not text/plain, it's $contentType. Ignoring")
                return
            }

            val chars = readChars(path)
            for (token in tokenizer.tokenize(chars)) {
                val entries = simpleIndex.computeIfAbsent(token.lexeme) { CopyOnWriteArrayList() }
            }
//            val tokens = tokenizer.tokenize(chars).groupBy({ it.lexeme}, { it.position })
//            for ((lexeme, positions) in tokens) {
//                val matches = index.computeIfAbsent(lexeme) { ConcurrentHashMap() }
////                matches[path] = positions
//            }
//            val attributes = Files.readAttributes(path, BasicFileAttributes::class.java)
//            documents[path] = Document(path, attributes)
        } catch (e: IOException) {
            log.error("Unable to read file $path: $e")
            log.debug("Exception thrown", e)
            return
        }
    }

    private fun updateDocument(path: Path) {
        deleteDocument(path)
        addDocument(path)
    }

    fun deleteDocument(path: Path) {
        log.debug("Deleting from index $path")
        val document = documents[path]
        if (document == null) {
            log.warn("Attempting to delete a nonexistent document $path")
            return
        }

        for (matches in index.values) {
            matches.remove(path)
        }
        documents.remove(path)
    }

    private fun readChars(path: Path) = sequence {
        // TODO: Handle character encoding
        val reader = path.toFile().bufferedReader()
        reader.use {
            var char = it.read()
            while (char != -1) {
                yield(char.toChar())
                char = it.read()
            }
        }
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
            filteredPaths.add(path)
        }
        return filteredPaths
    }

        private fun walkPaths(paths: Collection<Path>): List<Path> {
            val allPaths = ArrayList<Path>()
            for (path in paths) {
                Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
                    object : FileVisitor<Path> {
                        override fun preVisitDirectory(p: Path, attributes: BasicFileAttributes): FileVisitResult {
                            // TODO: Don't hardcode this.
                            if (p.fileName.toString() == ".git") {
                                log.info("Ignoring .git directory")
                                return FileVisitResult.SKIP_SUBTREE
                            }
                            log.debug("Scanning directory $p")
                            return FileVisitResult.CONTINUE
                        }

                        override fun visitFile(p: Path, attributes: BasicFileAttributes): FileVisitResult {
                            log.debug("Found regular file $p")
                            allPaths.add(p)
                            return FileVisitResult.CONTINUE
                        }

                        override fun visitFileFailed(p: Path, e: IOException?): FileVisitResult {
                            log.warn("Unable to access file $p: $e")
                            log.debug("Exception raised", e)
                            return FileVisitResult.CONTINUE;
                        }

                        override fun postVisitDirectory(p: Path, e: IOException?): FileVisitResult {
                            if (e != null) {
                                log.warn("Error after accessing directory $p")
                                log.debug("Exception raised", e)
                            }
                            return FileVisitResult.CONTINUE
                        }
                    })
            }
            return allPaths
        }
}

sealed class Event
class IndexUpdateEvent(): Event()
data class IndexingProgressEvent(val progress: Int, val done: Boolean): Event()
