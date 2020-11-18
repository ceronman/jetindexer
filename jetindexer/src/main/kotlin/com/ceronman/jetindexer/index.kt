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

// These are some constants with some sane defaults regarding index shard sizes.

// A size hint of how big a shard should be. It's just a hint, the shard might end up being slightly bigger
// because a shard must have all of the tokens of a given file.
private const val SHARD_SIZE_HINT = 50_000_000

// A size hint for the in-memory indexer hashmap. This is the number of tokens that should be indexed before
// dumping the shard to disk.
private const val SHARD_INDEX_CAPACITY = 100_000

// This is the minimum batch size to parallelize when indexing a batch of documents.
private const val MIN_CHUNK_SIZE = 1000

// The number of workers that will index a batch in parallel.
private val MAX_WORKERS = Runtime.getRuntime().availableProcessors()

/**
 * This is the callback that is passed to an indexing process. It will be called with a number
 * indicating the progress of the indexing. Being from 0 to 100.
 */
typealias ProgressCallback = ((Int) -> Unit)?

/**
 * Represents an inverted index of tokens to postings.
 *
 * Tokens are created from a text file using a [Tokenizer]. The inverted index is basically
 * a table mapping a token, which is a string, to a list of file IDs and positions, these
 * are called posting lists. We can use this to quickly look up which files have a given token and in
 * which position in those files.
 *
 * The tokens and postings are initially stored in memory, but after a number of them are collected,
 * they are stored in a [Shard].
 *
 * Shards are immutable. If we want to update or delete a file from the index, we just remove the
 * document id from the internal list of documents and add it as a new file with a new file id.
 *
 * Queries to the index should take care of verifying that the results are still officially part of
 * the index. This is done by a higher level interface such as [QueryResolver].
 *
 * @param tokenizer A [Tokenizer] object that will be used to split the file in tokens.
 *
 * Objects of this class are thread safe. Queries and updates to the index can happen from multiple
 * thread simultaneously.
 */
class InvertedIndex(private val tokenizer: Tokenizer) {
    private val idGenerator = AtomicInteger(1)
    private val log = LoggerFactory.getLogger(javaClass)
    private val documentsById = HashMap<Int, Document>()
    private val documentsByPath = HashMap<Path, Document>()
    private val shards = ArrayList<Shard>()
    private val shardWriter = ShardWriter(tokenizer)
    private val rwLock = ReentrantReadWriteLock()

    /**
     * Adds a batch of documents to the index.
     *
     * If the batch is big enough, the indexing will happen in parallel by multiple coroutines.
     * @param paths A collection of [Path] objects for files that should be indexed.
     * @param progressCallback A callback function accepting an integer which represents
     *  the progress of the indexing process. The number goes from 1 to 100.
     */
    fun addBatch(paths: Collection<Path>, progressCallback: ProgressCallback = null) = runBlocking {
        val newDocuments = paths.map(::createDocument)

        val chunkSize = max(newDocuments.size / MAX_WORKERS, MIN_CHUNK_SIZE)
        val chunks = newDocuments.chunked(chunkSize)

        log.info("Adding {} documents in {} chunks of {}", newDocuments.size, chunks.size, chunkSize)

        val jobs = ArrayList<Job>()
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
        jobs.joinAll()
        progressCallback?.invoke(100)
    }

    /**
     * Add a single file to the index.
     * @param path A [Path] object representing the file to index.
     */
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

    /**
     * Deletes a single file to the index.
     *
     * Index shards are immutable, so the files are not fully removed from the index. But they
     * are removed form the list of active documents. Filtering should be done when querying.
     *
     * @param path A [Path] object representing the file to remove.
     */
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

    /**
     * Updates a file from the index.
     *
     * This is done by deleting the file and adding it again.
     *
     * @see [update]
     * @see [delete]
     *
     * @param path A [Path] object representing the file to update.
     */
    fun update(path: Path) = rwLock.write {
        log.info("Will update {} to the index", path)
        if (documentsByPath.containsKey(path)) {
            delete(path)
            add(path)
        } else {
            log.warn("Attempting to update document {} that is not in the index", path)
        }
    }

    /**
     * Queries a token into the index. This method should not be used directly, but rather
     * throuh a [QueryResolver] which will use it appropriately.
     *
     * @return A [PostingListView] object which represent a continuous view into the multiple
     * shards containing the posting (file id + position) of the files containing the given token.
     */
    fun rawQuery(term: String): PostingListView = rwLock.read {
        val buffers = shards.mapNotNull { it.getPostingSlice(term) }.toMutableList()
        val posting = shardWriter.getPostingSlice(term)
        if (posting != null) {
            buffers.add(posting)
        }
        return PostingListView(buffers)
    }

    /**
     * Returns the document ID for a given path in the index. Or null if the document is not indexed.
     *
     * This is used by the [QueryResolver] to filter out deleted files.
     *
     * @param docId an document ID.
     * @return a [Path] object for the given file id.
     */
    fun documentPath(docId: Int): Path? = rwLock.read {
        return documentsById[docId]?.path
    }

    /**
     * Convenience method to create a new document.
     * Thread safety warning: Synchronization is required before calling this private method.
     */
    private fun createDocument(path: Path): Document {
        val doc = Document(idGenerator.getAndIncrement(), path)
        documentsById[doc.id] = doc
        documentsByPath[path] = doc
        return doc
    }

    /**
     * A job that will index a batch of documents. Likely going to be called from multiple coroutines or threads
     * running in parallel.
     */
    private fun addBatchJob(documents: Collection<Document>, progressCallback: ProgressCallback) {
        val index = ShardWriter(tokenizer)
        for (doc in documents) {
            index.add(doc)
            if (index.overCapacity()) {
                addShard(index.writeAndClear())
            }
            progressCallback?.invoke(doc.id)
        }
        if (index.hasDocuments()) {
            addShard(index.writeAndClear())
        }
    }

    private fun addShard(shard: Shard) = rwLock.write {
        shard.load()
        shards.add(shard)
    }
}

/**
 * Convenience class that represents a document.
 */
data class Document(val id: Int, val path: Path)

/**
 * A shard writer is used to write tokens and their locations.
 *
 * The writer stores an in-memory map of token to a list of files and positions (posting list).
 * when the writer reaches certain capacity, it can be dumped to disk to save memory. and that's
 * when a [Shard] is created.
 */
internal class ShardWriter(private val tokenizer: Tokenizer) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val postings = HashMap<String, PostingList>(SHARD_INDEX_CAPACITY)
    private val tokenPositions = HashMap<String, MutableList<Int>>(2048)
    private var size = 0

    /**
     * Adds a new [Document] to the shard
     */
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

    /**
     * Returns true if the writer has reached the configured capacity. In this case
     * the caller should write the shard to disk.
     */
    fun overCapacity(): Boolean {
        return size >= SHARD_SIZE_HINT || postings.size >= SHARD_INDEX_CAPACITY
    }

    /**
     * Returns true if the writer has any documents.
     */
    fun hasDocuments(): Boolean {
        return postings.size > 0
    }

    /**
     * Returns a [ByteBuffer] object representing the [PostingList] object for a given token
     *  or null if the writer doesn't contain the token.
     */
    fun getPostingSlice(token: String): ByteBuffer? {
        val posting = postings[token] ?: return null
        return posting.asReadOnly()
    }

    /**
     * Writes the existing tokens and postings to disk to create a new shard.
     * Clears the internal state so that new tokens can be indexed.
     */
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

/**
 * This represents an index shard. A shard is a file on disk containing a fraction of the
 * inverted index used by [InvertedIndex].
 *
 * To be able to search a token in the shard, the shard must be loaded. Currently, th
 * implementation is very limited and it directly loads the shard in memory.
 *
 * Ideally, this should not load the entire shard but just a fraction of it using a memory
 * mapped file or some similar strategy.
 *
 * The shard also contains an internal map of token to offsets in the file. These are used
 * to get a slice of the bytes particular posting list for a given token.
 *
 * @see [PostingList]
 * @see [PostingListView]
 * @see [InvertedIndex]
 */
internal class Shard(
    private val path: Path,
    private val tokenOffsets: Map<String, IntArray>
) {

    private lateinit var buffer: ByteBuffer

    /**
     * Loads the entire shard file into memory.
     *
     * This could be improved by using memory mapped files. Or not loading to memory at all.
     */
    fun load() {
        val bytes = Files.readAllBytes(path)
        Files.deleteIfExists(path)
        buffer = ByteBuffer.wrap(bytes)
    }

    /**
     * Returns a [ByteBuffer] object representing the [PostingList] object for a given token
     *  or null if the writer doesn't contain the token.
     */
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