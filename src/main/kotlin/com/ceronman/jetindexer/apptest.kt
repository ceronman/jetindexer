package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.SeekableByteChannel
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.system.measureTimeMillis

private val log = LoggerFactory.getLogger("app")

data class Doc(val path: Path, val id: Int)

fun main(args: Array<String>) = runBlocking {
    val path = Paths.get("/home/ceronman/code/github/intellij-community")
//    val path = Paths.get("/home/ceronman/code/github/linux")
//    val path = Paths.get("/home/ceronman/code/loxido")
//    val path = Paths.get("/home/ceronman/problems")
    var index: ShardedIndex
    var docs: List<Doc>
    println("Indexing $path")
    Files.walk(Paths.get("/home/ceronman/indexes")).forEach {
        if (it != Paths.get("/home/ceronman/indexes")) {
            Files.deleteIfExists(it)
        }
    }
    var time = measureTimeMillis {
        val filePaths = walkPaths(path)
        docs = filePaths.withIndex().map { (i, p) -> Doc(p, i + 1)}
        val chunkSize = 2000
        val chunks= docs.chunked(chunkSize)
        println("Using ${chunks.size} chunks of size $chunkSize")

        val jobs = ArrayList<Deferred<Shard>>()
        for ((i, chunk) in docs.chunked(chunkSize).withIndex()) {
            val job = async(Dispatchers.Default) {
                index2(i, chunk)
            }
            jobs.add(job)
        }
        val shards = jobs.awaitAll()
        index = ShardedIndex(shards)

        println("Jobs done")
    }
    println("Took $time milliseconds (${time.toDouble() / 1000.0} seconds")

    val t = measureTimeMillis {
        index.search("ParameterTableModelItemBase")
    }

    println("1000 queries took $t milliseconds")

    val pathsByDocId = docs.map { it.id to it.path }.toMap()
    val result = index.search("ParameterTableModelItemBase")
    val resultsByFile = result.groupBy { it.docId }
    for ((fileId, posting) in resultsByFile) {
        println(pathsByDocId[fileId])
        for (p in posting) {
            println(p.position)
        }
    }
}

private fun index2(shardId: Int, docs: Collection<Doc>): Shard {
    val index = HashMap<String, BinPosting>()
    val tokenizer = BetterTrigramTokenizer2()
    val tokenPositions = HashMap<String, MutableList<Int>>()
    for (doc in docs) {
        tokenPositions.clear()
        for (token in tokenizer.tokenize(doc.path)) {
            tokenPositions.computeIfAbsent(token.lexeme) { ArrayList() }.add(token.position)
        }
        for ((token, positions) in tokenPositions) {
            val posting = index.computeIfAbsent(token) { BinPosting() }
            posting.put(doc.id, positions)
        }
    }

    val shard = Shard(shardId)
    Files.createFile(shard.path)
    val sortedTokens = index.keys.sorted()
    Files.newByteChannel(shard.path, StandardOpenOption.WRITE).use { channel ->
        for (t in sortedTokens) {
            val posting = index[t]!!.close()
            shard.postingOffsets[t] = channel.position().toInt()
            channel.write(posting)
        }
        channel.write(ByteBuffer.allocate(1).put(0))
    }

    return shard
}

class Shard(id: Int) {
    val path: Path = Paths.get("/home/ceronman/indexes/$id.bin")
    val postingOffsets = HashMap<String, Int>()
}

private fun index(docs: Collection<Doc>): ByteArray {
    val index = index4(docs)
    val cap = 5_000_000
    var buffer = ByteBuffer.allocate(cap)
    for (token in index.keys.sorted()) {
        val docIds = index[token]!!.sorted()
        val posting = Posting(token, docIds)
        try {
            posting.writeTo(buffer)
        } catch (e: PostingBufferOverflowException) {
            val newBuffer = ByteBuffer.allocate(buffer.capacity() + cap)
            buffer.flip()
            newBuffer.put(buffer)
            buffer = newBuffer
            posting.writeTo(buffer)
        }
    }
    buffer.flip()
    val result = ByteArray(buffer.limit())
    buffer.get(result)
    return result
}

private fun index4(docs: Collection<Doc>): Map<String, List<Int>> {
    val index = HashMap<String, MutableList<Int>>()
    val tokenizer = BetterTrigramTokenizer()
    for (doc in docs) {
        val tokens = tokenizer.tokenize(doc.path).toSet()
        for (token in tokens) {
            index.computeIfAbsent(token){ ArrayList() }.add(doc.id)
        }
    }
    return index
}

class BetterTrigramTokenizer {
    private val decoder = Charsets.UTF_8.newDecoder()
    private val log = LoggerFactory.getLogger(javaClass)

    fun tokenize(path: Path): Sequence<String> {
        val bytes = Files.readAllBytes(path)
        val string = try {
            decoder.decode(ByteBuffer.wrap(bytes)).toString()
        } catch (e: CharacterCodingException) {
            log.warn("File $path contains malformed unicode, ignoring")
            return emptySequence()
        }
        return tokenize(string)
    }

    fun tokenize(str: String): Sequence<String> = sequence {
        if (str.codePointCount(0, str.length) < 3) {
            return@sequence
        }

        var start = 0
        var end = str.offsetByCodePoints(start, 3)
        while (end < str.length) {
            yield(str.substring(start, end))
            start = str.offsetByCodePoints(start, 1)
            end = str.offsetByCodePoints(start, 3)
        }
    }
}

class BetterTrigramTokenizer2 {
    private val decoder = Charsets.UTF_8.newDecoder()
    private val log = LoggerFactory.getLogger(javaClass)

    fun tokenize(path: Path): Sequence<Token> {
        val bytes = Files.readAllBytes(path)
        val string = try {
            decoder.decode(ByteBuffer.wrap(bytes)).toString()
        } catch (e: CharacterCodingException) {
            log.warn("File $path contains malformed unicode, ignoring")
            return emptySequence()
        }
        return tokenize(string)
    }

    fun tokenize(str: String): Sequence<Token> = sequence {
        if (str.codePointCount(0, str.length) < 3) {
            return@sequence
        }

        var start = 0
        var end = str.offsetByCodePoints(start, 3)
        while (end < str.length) {
            yield(Token(str.substring(start, end), start))
            start = str.offsetByCodePoints(start, 1)
            end = str.offsetByCodePoints(start, 3)
        }
    }
}

class FastTrigramTokenizer {
    private val decoder = Charsets.UTF_8.newDecoder()
    private val log = LoggerFactory.getLogger(javaClass)

    fun tokenize(path: Path): Set<String> {
        val bytes = Files.readAllBytes(path)
        val string = String(bytes, Charsets.UTF_8)
        val tokens = HashSet<String>()
        for (i in 2 until string.length - 3) {
            val token = string.substring(i, i + 3)
            tokens.add(token)
        }
        return tokens
    }
}
class WsTokenizer {
    private val decoder = Charsets.UTF_8.newDecoder()
    private val log = LoggerFactory.getLogger(javaClass)

    fun tokenize(path: Path): Set<String> {
        val bytes = Files.readAllBytes(path)
        val string = String(bytes, Charsets.UTF_8)
        val token = StringBuilder()
        var position = 0
        val tokens = HashSet<String>()
        for (c in string) {
            if (c.isWhitespace()) {
                if (token.isNotEmpty()) {
                    tokens.add(token.toString())
                    token.clear()
                }
            } else {
                token.append(c)
            }
            position++
        }
        if (token.isNotEmpty()) {
            tokens.add(token.toString())
        }
        return tokens
    }

}

private fun walkPaths(path: Path): List<Path> {
    val allPaths = ArrayList<Path>()
    Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
            object : FileVisitor<Path> {
                override fun preVisitDirectory(p: Path, attributes: BasicFileAttributes): FileVisitResult {
                    // TODO: Don't hardcode this.
                    if (p.fileName.toString() == ".git") {
                        return FileVisitResult.SKIP_SUBTREE
                    }
                    return FileVisitResult.CONTINUE
                }

                override fun visitFile(p: Path, attributes: BasicFileAttributes): FileVisitResult {
                    val contentType = Files.probeContentType(p)
                    if (!contentType.contains("text")) {
                        return FileVisitResult.CONTINUE
                    }
                    if (Files.size(p) > 50_000_000) {
                        log.error("File to big $p")
                        return FileVisitResult.CONTINUE
                    }
                    allPaths.add(p)
                    return FileVisitResult.CONTINUE
                }

                override fun visitFileFailed(p: Path, e: IOException?): FileVisitResult {
                    return FileVisitResult.CONTINUE;
                }

                override fun postVisitDirectory(p: Path, e: IOException?): FileVisitResult {
                    return FileVisitResult.CONTINUE
                }
            })
    return allPaths
}

class InvertedIndex(val tokens: Map<String, Int>, val postings: ByteArray) {

    fun query(term: String): List<Int> {
        val offset = tokens[term] ?: return emptyList()
        val buffer = ByteBuffer.wrap(postings)
        buffer.position(offset)
        val posting = Posting.readFrom(buffer) ?: return emptyList()
        return posting.documents
    }

    fun search(term: String): List<Int> {
        val trigrams = BetterTrigramTokenizer().tokenize(term)
        val results = trigrams.map(::query).toMutableList()
        if (results.isEmpty()) {
            return emptyList()
        }

        var merged = results.removeLast()
        while (results.isNotEmpty()) {
            merged = sortedListIntersection(merged, results.removeLast())
        }
        return merged
    }

    private fun sortedListIntersection(l1: List<Int>, l2: List<Int>): List<Int> {
        val result = ArrayList<Int>()
        var i = 0
        var j = 0
        while (i < l1.size && j < l2.size) {
            if (l1[i] < l2[j]) {
                i++
            } else if (l2[j] < l1[i]) {
                j++
            } else {
                result.add(l2[j])
                j++
                i++
            }
        }
        return result
    }

    companion object {
        fun fromPostings(postings: Collection<ByteArray>): InvertedIndex {
            val tokens = HashMap<String, Int>()
            val cap = 100_000_000
            var outBuffer = ByteBuffer.allocate(cap)
            val buffers = postings.map { ByteBuffer.wrap(it) }
            val postings = buffers.map { Posting.readFrom(it) }.toMutableList()
            while (postings.any { it != null}) {
                val token = postings.filterNotNull().minByOrNull { it.token }!!.token
                val docIds = ArrayList<Int>()
                for (i in 0 until postings.size) {
                    val posting = postings[i] ?: continue

                    if (posting.token == token) {
                        docIds.addAll(posting.documents)
                        postings[i] = Posting.readFrom(buffers[i])
                    }
                }
                tokens[token] = outBuffer.position()
                val posting = Posting(token, docIds)
                try {
                    posting.writeTo(outBuffer)
                } catch (e: PostingBufferOverflowException) {
                    val newBuffer = ByteBuffer.allocate(outBuffer.capacity() + cap)
                    outBuffer.flip()
                    newBuffer.put(outBuffer)
                    outBuffer = newBuffer
                    posting.writeTo(outBuffer)
                    println("Growing")
                }
            }
            outBuffer.flip()
            val result = ByteArray(outBuffer.limit())
            outBuffer.get(result)
            println("Size ${result.size}")
            return InvertedIndex(tokens, result)
        }
    }
}

class PostingBufferOverflowException(message: String): Exception(message)

data class Posting(val token: String, val documents: List<Int>) {
    fun writeTo(buffer: ByteBuffer) {
        val tokenBytes = this.token.toByteArray(Charsets.UTF_8)
        val docIds = this.documents
        if (buffer.remaining() >= (8 + tokenBytes.size + Int.SIZE_BYTES * docIds.size)) {
            buffer.putInt(tokenBytes.size)
            buffer.put(tokenBytes)
            buffer.putInt(docIds.size)
            for (docId in docIds.sorted()) {
                buffer.putInt(docId)
            }
        } else {
            throw PostingBufferOverflowException("Limit exceeded")
        }
    }

    companion object {
        fun readFrom(buffer: ByteBuffer): Posting? {
            if (!buffer.hasRemaining()) {
                return null
            }
            val tokenSize = buffer.int
            val tokenBytes = ByteArray(tokenSize)
            buffer.get(tokenBytes)
            val numDocs = buffer.int
            val docs = ArrayList<Int>()
            for (i in 0 until numDocs) {
                docs.add(buffer.int)
            }
            return Posting(String(tokenBytes, Charsets.UTF_8), docs)
        }
    }
}

const val VAR_INT_MAX_SIZE = 5

class BinPosting() {
    private var buffer = ByteBuffer.allocate(1024)

    fun close(): ByteBuffer {
        growIfNeeded(VAR_INT_MAX_SIZE)
        buffer.putVarInt(0)
        buffer.flip()
        return buffer.asReadOnlyBuffer()
    }

    fun put(docId: Int, positions: List<Int>) {
        growIfNeeded(VAR_INT_MAX_SIZE * 2)
        buffer.putVarInt(docId)
        buffer.putVarInt(positions.size)
        var prev = 0
        for (position in positions) {
            growIfNeeded(VAR_INT_MAX_SIZE)
            buffer.putVarInt(position - prev)
            prev = position
        }
    }

    private fun growIfNeeded(required: Int) {
        if (buffer.remaining() < required) {
            val newBuffer = ByteBuffer.allocate((buffer.capacity() * 1.5).toInt())
            buffer.flip()
            newBuffer.put(buffer)
            buffer = newBuffer
        }
    }
}

data class TokenPosting(val docId: Int, val position: Int)

fun mergePostings(postings: List<List<TokenPosting>>): List<TokenPosting> {
    val numLists = postings.size
    val result = ArrayList<TokenPosting>()
    val iterators = postings.map { PeekableIterator(it) }
    while (iterators.all { it.hasNext() }) {
        val firstPosting = iterators[0].peek()
        var equals = true
        for (i in 1 until numLists) {
            val currentPosting = iterators[i].peek()
            if (firstPosting.docId < currentPosting.docId) {
                iterators[0].next()
            } else if (firstPosting.docId > currentPosting.docId) {
                iterators[i].next()
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

class PeekableIterator<T>(private val list: List<T>): Iterator<T> {
    private var i = 0

    override fun hasNext(): Boolean {
        return i < list.size
    }

    override fun next(): T {
        val result = list[i]
        i++
        return result
    }

    fun peek(): T {
        return list[i]
    }
}

private fun sortedListIntersection(l1: List<Int>, l2: List<Int>): List<Int> {
    val result = ArrayList<Int>()
    var i = 0
    var j = 0
    while (i < l1.size && j < l2.size) {
        if (l1[i] < l2[j]) {
            i++
        } else if (l2[j] < l1[i]) {
            j++
        } else {
            result.add(l2[j])
            j++
            i++
        }
    }
    return result
}

class ShardedIndex(private val shards: List<Shard>): AutoCloseable {
    private val channels: List<FileChannel>
    private val buffers: List<ByteBuffer>
    init {
        try {
            channels = shards.map { Files.newByteChannel(it.path, StandardOpenOption.READ) as FileChannel }
            buffers = channels.map { it.map(FileChannel.MapMode.READ_ONLY, 0, it.size()) }
        } catch (e: IOException) {
            throw e
        }
    }

    fun search(term: String): List<TokenPosting> {
        val trigrams = BetterTrigramTokenizer2().tokenize(term).map { it.lexeme }
        val postings = trigrams.map(::query).toMutableList()
        if (postings.isEmpty()) {
            return emptyList()
        }
        return mergePostings(postings)
    }

    fun query(token: String): List<TokenPosting> {
        val result = ArrayList<TokenPosting>()
        for ((i, shard) in shards.withIndex()) {
            val offset = shard.postingOffsets[token] ?: continue
            val buffer = buffers[i]
            buffer.position(offset)
            var docId = buffer.getVarInt()
            while (docId != 0) {
                val numPositions = buffer.getVarInt()
                var prev = 0
                for (j in 0 until numPositions) {
                    val position = buffer.getVarInt() + prev
                    result.add(TokenPosting(docId, position))
                    prev = position
                }
                docId = buffer.getVarInt()
            }
        }
        return result
    }

    override fun close() {
        channels.forEach(SeekableByteChannel::close)
    }
}

fun ByteBuffer.putVarInt(value: Int): ByteBuffer {
    var v = value
    while (true) {
        val bits = v and 0x7f
        v = v ushr 7
        if (v == 0) {
            this.put(bits.toByte())
            return this
        }
        this.put((bits or 0x80).toByte())
    }
    return this
}

fun ByteBuffer.getVarInt(): Int {
    var tmp = this.get().toInt()
    if (tmp  >= 0) {
        return tmp
    }
    var result = tmp and 0x7f
    tmp = this.get().toInt()
    if (tmp >= 0) {
        result = result or (tmp shl 7)
    } else {
        result = result or (tmp and 0x7f shl 7)
        tmp = this.get().toInt()
        if (tmp >= 0) {
            result = result or (tmp shl 14)
        } else {
            result = result or (tmp and 0x7f shl 14)
            tmp = this.get().toInt()
            if (tmp >= 0) {
                result = result or (tmp shl 21)
            } else {
                result = result or (tmp and 0x7f shl 21)
                tmp = this.get().toInt()
                result = result or (tmp shl 28)
            }
        }
    }
    return result
}