package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.file.Path

const val SHARD_SIZE_HINT = 2_000_000
const val SHARD_INDEX_CAPACITY = 100_000

class TokenIndex(paths: Collection<Path>) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val documents: Map<Int, Doc>
    private lateinit var shards: List<Shard>

    init {
        documents = paths.withIndex()
                .map { (id, path) -> Doc(id + 1, path) }
                .map { it.id to it }
                .toMap()
    }

    fun index() = runBlocking {
        val chunkSize = Math.max(documents.size / 7, 1000)
        val chunks = documents.values.chunked(chunkSize)
        log.info("Indexing {} documents in chunks of {}", documents.size, chunkSize)

        val jobs = ArrayList<Deferred<List<Shard>>>()
        for (chunk in chunks) {
            val job = async(Dispatchers.Default) {
                Shard.create(chunk)
            }
            jobs.add(job)
        }
        shards = jobs.awaitAll().flatten()
    }

    fun search(term: String): List<QueryResult> {
        val trigrams = TriTokenizer().tokenize(term)

        val postings: List<PostingListView>
        postings = trigrams.map { query(it.lexeme) }.toList()

        if (postings.isEmpty()) {
            return emptyList()
        }

        return mergePostings(postings).map { QueryResult(term, documents[it.docId]!!.path, it.position) }
    }

    private fun query(term: String): PostingListView {
        val buffers = shards
                .map { it.getPostingSlice(term) }
                .filterNotNull()
        return PostingListView(buffers)
    }

    private fun mergePostings(postings: List<PostingListView>): List<Posting> {
        val numLists = postings.size
        val result = ArrayList<Posting>()
        val iterators = postings.map { it.peekableIterator() }
        while (iterators.all { it.hasNext() }) {
            val firstPosting = iterators[0].peek()
            var equals = true
            for (i in 1 until numLists) {
                val currentPosting = iterators[i].peek()
                if (firstPosting.docId < currentPosting.docId) {
                    while (iterators[0].hasNext() && iterators[0].peek().docId < currentPosting.docId) {
                        iterators[0].next()
                    }
                } else if (currentPosting.docId < firstPosting.docId) {
                    while (iterators[i].hasNext() && iterators[i].peek().docId < firstPosting.docId) {
                        iterators[i].next()
                    }
                } else if (firstPosting.position < currentPosting.position - i) {
                    iterators[0].next()
                } else if (firstPosting.position > currentPosting.position - i) {
                    iterators[i].next()
                } else {
                    continue
                }
                equals = false
                break
            }
            if (equals) {
                iterators.forEach { it.next() }
                result.add(firstPosting)
            }
        }
        return result
    }
}

internal class Shard private constructor(
        private val buffer: ByteBuffer,
        private val tokenOffsets: Map<String, IntArray>) {

    fun getPostingSlice(token: String): ByteBuffer? {
        val offset = tokenOffsets[token] ?: return null
        val position = offset[0]
        val size = offset[1]
        buffer.position(position)
        val slice = buffer.slice()
        slice.limit(size)
        return slice
    }

    companion object {
        private fun allocateShard(size: Int, postings: HashMap<String, PostingList>): Shard {
            val tokenOffsets = HashMap<String, IntArray>(postings.size)
            val buffer = ByteBuffer.allocate(size + postings.size + 1)
            for ((token, postingList) in postings) {
                val postingListBuffer = postingList.close()
                val offset = IntArray(2)
                offset[0] = buffer.position()
                offset[1] = postingListBuffer.remaining()
                tokenOffsets[token] = offset
                buffer.put(postingListBuffer)
            }
            // Mark the end of the postings
            buffer.put(0)
            return Shard(buffer, tokenOffsets)
        }

        fun create(docs: Collection<Doc>): List<Shard> {
            val index = HashMap<String, PostingList>(SHARD_INDEX_CAPACITY)
            val shards = ArrayList<Shard>()
            val tokenizer = TriTokenizer()
            val tokenPositions = HashMap<String, MutableList<Int>>()
            var shardSize = 0
            for (doc in docs) {
                tokenPositions.clear()
                for (token in tokenizer.tokenize(doc.path)) {
                    tokenPositions.computeIfAbsent(token.lexeme) { ArrayList() }.add(token.position)
                }
                for ((token, positions) in tokenPositions) {
                    val posting = index.computeIfAbsent(token) { PostingList() }
                    val bytesWritten = posting.put(doc.id, positions)
                    shardSize += bytesWritten
                }

                if (shardSize >= SHARD_SIZE_HINT) {
                    shards.add(allocateShard(shardSize, index))
                    index.clear()
                    shardSize = 0
                }
            }

            if (index.isNotEmpty()) {
                shards.add(allocateShard(shardSize, index))
            }

            return shards
        }
    }
}