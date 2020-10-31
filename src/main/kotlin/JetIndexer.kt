import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
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
    val indexingProgress: ReceiveChannel<Float> get() = _indexingProgress
    
    private val _indexingProgress = Channel<Float>()
    private val log = LoggerFactory.getLogger(this.javaClass)
    private val index = ConcurrentHashMap<String, MutableSet<Path>>()
    private val documents = ConcurrentHashMap<Path, Document>()

    suspend fun index() = withContext(Dispatchers.Default) {
        val allPaths = walkPaths()
        for ((i, path) in allPaths.withIndex()) {
            val progress = i.toFloat() / allPaths.size.toFloat()
            _indexingProgress.send(progress)
            addDocument(path)
        }
        _indexingProgress.send(1.0f)
        _indexingProgress.close()

        // TODO: Initiate file watching
        Thread.sleep(100000)
    }

    fun stop(): Nothing = TODO()

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

    private fun addDocument(path: Path) {
        log.debug("Adding $path")

        try {
            // TODO: test this
            val contentType = Files.probeContentType(path)
            if (contentType != "text/plain") {
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

    // TODO: This is temporarily public to be able to test
    fun updateDocument(path: Path) {
        deleteDocument(path)
        addDocument(path)
    }

    // TODO: This is temporarily public to be able to test
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

    private fun walkPaths(): List<Path> {
        val allPaths = ArrayList<Path>()
        for (path in paths) {
            Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
                object : FileVisitor<Path> {
                    override fun preVisitDirectory(p: Path, attributes: BasicFileAttributes): FileVisitResult {
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

data class QueryResult(
    val term: String,
    val path: Path,
    val position: Int
)