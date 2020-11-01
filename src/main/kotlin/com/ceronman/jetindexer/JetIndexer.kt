package com.ceronman.jetindexer

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class JetIndexer(
    private val tokenizer: Tokenizer,
    private val paths: Collection<Path>
) {
    private val eventsChannel = ConflatedBroadcastChannel<Event>()
    private val log = LoggerFactory.getLogger(this.javaClass)
    private val index = ConcurrentHashMap<String, MutableSet<Path>>()
    private val documents = ConcurrentHashMap<Path, Document>()
    @Volatile private var watcher: DirectoryWatcher? = null

    suspend fun index() = withContext(Dispatchers.Default) {
        val directoryPaths = filterPaths(paths)
        val filePaths = walkPaths(directoryPaths)
        for ((i, path) in filePaths.withIndex()) {
            val progress = (i.toFloat() / filePaths.size.toFloat()) * 100.0
            eventsChannel.send(IndexingProgressEvent(progress.toInt(), false))
            addDocument(path)
        }

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
                    runBlocking { eventsChannel.send(IndexUpdateEvent()) }
                }
            })
            .build()
        log.debug("Watcher was built")

        eventsChannel.send(IndexingProgressEvent(100, true))

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
        val paths = index[term] ?: return emptyList()

        val results = ArrayList<QueryResult>()
        for (path in paths) {
            val document = documents[path] ?: continue
            val positions = document.occurrences[term] ?: continue
            for (position in positions) {
                results.add(QueryResult(term, path, position))
            }
        }
        return results
    }

    fun events(): ReceiveChannel<Event> {
        return eventsChannel.openSubscription()
    }

    private fun addDocument(path: Path) {
        log.debug("Adding $path")

        try {
            // TODO: test this
            val contentType = Files.probeContentType(path)
            if (!contentType.contains("text")) {
                log.warn("File $path is not text/plain, it's $contentType. Ignoring")
                return
            }

            val chars = readChars(path)
            val tokens = tokenizer.tokenize(chars)
            val occurrences = HashMap<String, MutableList<Int>>()
            for (token in tokens) {
                log.debug("Processing ${token.lexeme} ${token.position}")
                val paths = index.computeIfAbsent(token.lexeme) { Collections.newSetFromMap(ConcurrentHashMap()) }
                paths.add(path)
                occurrences.computeIfAbsent(token.lexeme) { ArrayList() }.add(token.position)
            }
            val attributes = Files.readAttributes(path, BasicFileAttributes::class.java)
            documents[path] = Document(path, attributes, occurrences)
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
        for (term in document.occurrences.keys) {
            val paths = index[term]
            if (paths == null) {
                log.warn("Index doesn't contain terms in $path")
                continue
            }
            paths.remove(path)
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
