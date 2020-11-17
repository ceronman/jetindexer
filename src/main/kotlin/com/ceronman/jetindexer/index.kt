package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

const val SHARD_SIZE_HINT = 50_000_000
const val SHARD_INDEX_CAPACITY = 100_000
const val MIN_CHUNK_SIZE = 1000
private val MAX_WORKERS = Runtime.getRuntime().availableProcessors()

class TokenIndex(private val tokenizer: ITokenizer) {
    private val idGenerator = AtomicInteger(1)
    private val log = LoggerFactory.getLogger(javaClass)
    private val documentsById = HashMap<Int, Doc>()
    private val documentsByPath = HashMap<Path, Doc>()
    private val shards = ArrayList<Shard>()
    private val shardWriter = ShardWriter(tokenizer)

    private fun createDocument(path: Path): Doc {
        val doc = Doc(idGenerator.getAndIncrement(), path)
        documentsById[doc.id] = doc
        documentsByPath[path] = doc
        return doc
    }

    fun addBatch(paths: Collection<Path>) = runBlocking {
        val newDocuments = paths.map { createDocument(it) }

        val chunkSize = max(newDocuments.size / MAX_WORKERS, MIN_CHUNK_SIZE)
        val chunks = newDocuments.chunked(chunkSize)

        log.info("Indexing {} documents in ${chunks.size} chunks of {}", newDocuments.size, chunkSize)

        val jobs = ArrayList<Deferred<List<Shard>>>()
        for (chunk in chunks) {
            val job = async(Dispatchers.Default) {
                addBatchJob(chunk)
            }
            jobs.add(job)
        }
        val results = jobs.awaitAll().flatten()
        results.forEach { it.load() }
        shards.addAll(results)
    }

    private fun addBatchJob(documents: Collection<Doc>): List<Shard> {
        val shards = ArrayList<Shard>()
        val index = ShardWriter(tokenizer)
        for (doc in documents) {
            index.add(doc)
            if (index.overCapacity()) {
                shards.add(index.writeAndClear())
            }
        }
        if (index.hasDocuments()) {
            shards.add(index.writeAndClear())
        }
        return shards
    }

    fun add(path: Path) {
        val document = createDocument(path)
        shardWriter.add(document)

        if (shardWriter.overCapacity()) {
            log.info("ShardWriter is over capacity, writing shard to disk")
            val shard = shardWriter.writeAndClear()
            shard.load()
            shards.add(shard)
        }
    }

    fun delete(path: Path) {
        val document = documentsByPath[path]
        if (document == null) {
            log.warn("Attempting to delete a file not indexed: {}", path)
            return
        }
        documentsById.remove(document.id)
        documentsByPath.remove(path)
    }

    fun update(path: Path) {
        delete(path)
        add(path)
    }

    fun rawQuery(term: String): PostingListView {
        val buffers = shards.mapNotNull { it.getPostingSlice(term) }.toMutableList()
        val posting = shardWriter.getPostingSlice(term)
        if (posting != null) {
            buffers.add(posting)
        }
        return PostingListView(buffers)
    }

    fun documentPath(docId: Int): Path? {
        return documentsById[docId]?.path
    }
}

internal class ShardWriter(private val tokenizer: ITokenizer) {
    private val postings = HashMap<String, PostingList>(SHARD_INDEX_CAPACITY)
    private val tokenPositions = HashMap<String, MutableList<Int>>(2048)
    private var size = 0

    fun add(document: Doc) {
        tokenPositions.clear()
        for (token in tokenizer.tokenize(document.path)) {
            tokenPositions.computeIfAbsent(token.lexeme) { ArrayList() }.add(token.position)
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
                val postingListBuffer = postingList.close()
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
        private val tokenOffsets: Map<String, IntArray>) {

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
        buffer.position(position)
        val slice = buffer.slice()
        slice.limit(size)
        return slice
    }
}