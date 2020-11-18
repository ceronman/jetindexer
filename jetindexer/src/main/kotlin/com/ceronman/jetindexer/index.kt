package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.max

private const val SHARD_SIZE_HINT = 50_000_000
private const val SHARD_INDEX_CAPACITY = 100_000
private const val MIN_CHUNK_SIZE = 1000
private val MAX_WORKERS = Runtime.getRuntime().availableProcessors()

typealias ProgressCallback = ((Int) -> Unit)?

class TokenIndex(private val tokenizer: Tokenizer) {
    private val idGenerator = AtomicInteger(1)
    private val log = LoggerFactory.getLogger(javaClass)
    private val documentsById = HashMap<Int, Document>()
    private val documentsByPath = HashMap<Path, Document>()
    private val shards = ArrayList<Shard>()
    private val shardWriter = ShardWriter(tokenizer)
    private val rwLock = ReentrantReadWriteLock()

    private fun createDocument(path: Path): Document {
        val doc = Document(idGenerator.getAndIncrement(), path)
        documentsById[doc.id] = doc
        documentsByPath[path] = doc
        return doc
    }

    fun addBatch(paths: Collection<Path>, progressCallback: ProgressCallback = null) = runBlocking {
        val newDocuments = paths.map(::createDocument)

        val chunkSize = max(newDocuments.size / MAX_WORKERS, MIN_CHUNK_SIZE)
        val chunks = newDocuments.chunked(chunkSize)

        log.info("Adding {} documents in {} chunks of {}", newDocuments.size, chunks.size, chunkSize)

        val jobs = ArrayList<Deferred<List<Shard>>>()
        val progress = AtomicInteger()
        for (chunk in chunks) {
            val job = async(Dispatchers.Default) {
                addBatchJob(chunk) {
                    val p = (progress.incrementAndGet().toDouble() / newDocuments.size.toDouble()) * 100.0
                    progressCallback?.invoke(p.toInt())
                }
            }
            jobs.add(job)
        }
        val results = jobs.awaitAll().flatten()
        results.forEach(Shard::load)
        rwLock.write {
            shards.addAll(results)
        }
        progressCallback?.invoke(100)
    }

    private fun addBatchJob(documents: Collection<Document>, progressCallback: ProgressCallback): List<Shard> {
        val shards = ArrayList<Shard>()
        val index = ShardWriter(tokenizer)
        for (doc in documents) {
            index.add(doc)
            if (index.overCapacity()) {
                shards.add(index.writeAndClear())
            }
            progressCallback?.invoke(doc.id)
        }
        if (index.hasDocuments()) {
            shards.add(index.writeAndClear())
        }
        return shards
    }

    fun add(path: Path) = rwLock.write {
        log.info("Adding {} to the index", path)

        val document = createDocument(path)
        shardWriter.add(document)

        if (shardWriter.overCapacity()) {
            log.info("ShardWriter is over capacity, writing shard to disk")
            val shard = shardWriter.writeAndClear()
            shard.load()
            shards.add(shard)
        }
    }

    fun delete(path: Path) = rwLock.write {
        log.info("Removed {} to the index", path)
        val document = documentsByPath[path]
        if (document == null) {
            log.warn("Attempting to delete a file not indexed: {}", path)
        } else {
            documentsById.remove(document.id)
            documentsByPath.remove(path)
        }
    }

    fun update(path: Path) = rwLock.write {
        log.info("Will update {} to the index", path)
        if (documentsByPath.containsKey(path)) {
            delete(path)
            add(path)
        } else {
            log.warn("Attempting to update document {} that is not in the index", path)
        }
    }

    fun rawQuery(term: String): PostingListView = rwLock.read {
        val buffers = shards.mapNotNull { it.getPostingSlice(term) }.toMutableList()
        val posting = shardWriter.getPostingSlice(term)
        if (posting != null) {
            buffers.add(posting)
        }
        return PostingListView(buffers)
    }

    fun documentPath(docId: Int): Path? = rwLock.read {
        return documentsById[docId]?.path
    }
}

data class Document(val id: Int, val path: Path)

internal class ShardWriter(private val tokenizer: Tokenizer) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val postings = HashMap<String, PostingList>(SHARD_INDEX_CAPACITY)
    private val tokenPositions = HashMap<String, MutableList<Int>>(2048)
    private var size = 0

    fun add(document: Document) {
        tokenPositions.clear()
        val tokens = try {
            tokenizer.tokenize(document.path)
        } catch (e: Exception) {
            log.warn("Tokenizer failed for ${document.path}", e)
            log.debug("Exception raised", e)
            return
        }
        for (token in tokens) {
            tokenPositions.computeIfAbsent(token.text) { ArrayList() }.add(token.position)
        }
        for ((token, positions) in tokenPositions) {
            val posting = postings.computeIfAbsent(token) { PostingList() }
            val bytesWritten = posting.put(document.id, positions)
            size += bytesWritten
        }
    }

    fun overCapacity(): Boolean {
        return size >= SHARD_SIZE_HINT || postings.size >= SHARD_INDEX_CAPACITY
    }

    fun hasDocuments(): Boolean {
        return postings.size > 0
    }

    fun getPostingSlice(token: String): ByteBuffer? {
        val posting = postings[token] ?: return null
        return posting.asReadOnly()
    }

    fun writeAndClear(): Shard {
        val shardPath = Files.createTempFile("jetindexer", "shard")
        val tokenOffsets = HashMap<String, IntArray>(postings.size)
        Files.newByteChannel(shardPath, StandardOpenOption.WRITE).use { channel ->
            for ((token, postingList) in postings) {
                val postingListBuffer = postingList.asReadOnly()
                val offset = IntArray(2)
                offset[0] = channel.position().toInt()
                offset[1] = postingListBuffer.remaining()
                tokenOffsets[token] = offset
                channel.write(postingListBuffer)
            }
        }
        postings.clear()
        size = 0
        return Shard(shardPath, tokenOffsets)
    }
}

internal class Shard(
    private val path: Path,
    private val tokenOffsets: Map<String, IntArray>
) {

    private lateinit var buffer: ByteBuffer

    fun load() {
        val bytes = Files.readAllBytes(path)
        Files.deleteIfExists(path)
        buffer = ByteBuffer.wrap(bytes)
    }

    fun getPostingSlice(token: String): ByteBuffer? {
        val offset = tokenOffsets[token] ?: return null
        val position = offset[0]
        val size = offset[1]
        val copy = buffer.duplicate()
        copy.position(position)
        val slice = copy.slice()
        slice.limit(size)
        return slice
    }
}