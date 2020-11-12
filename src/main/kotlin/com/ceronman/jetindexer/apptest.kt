package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
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
    Files.walk(Paths.get("/home/ceronman/indexes")).forEach {
        if (it != Paths.get("/home/ceronman/indexes")) {
            Files.deleteIfExists(it)
        }
    }
    val path = Paths.get("/home/ceronman/code/github/intellij-community")
//    val path = Paths.get("/home/ceronman/code/github/linux")
//    val path = Paths.get("/home/ceronman/code/loxido")
//    val path = Paths.get("/home/ceronman/problems")
    var index: InvertedIndex

    println("Indexing $path")
    var time = measureTimeMillis {
        val filePaths = walkPaths(path)
        val docs = filePaths.withIndex().map { (i, p) -> Doc(p, i)}
        val chunkSize = 2000
        val chunks= docs.chunked(chunkSize)
        println("Using ${chunks.size} chunks of size $chunkSize")

        val jobs = ArrayList<Job>()
        for ((i, chunk) in docs.chunked(chunkSize).withIndex()) {
            val job = async(Dispatchers.Default) {
                index2(i, chunk)
            }
            jobs.add(job)
        }
        jobs.joinAll()


//        println("Got ${docs.size} paths")
//        val chunkSize = 1000
//        val chunks= docs.chunked(chunkSize)
//        println("Using ${chunks.size} chunks of size $chunkSize")
//
//        val jobs = ArrayList<Deferred<ByteArray>>()
//        for (chunk in docs.chunked(chunkSize)) {
//            val job = async(Dispatchers.Default) {
//                index(chunk)
//            }
//            jobs.add(job)
//        }
//        val results = jobs.awaitAll()
        println("Jobs done")

//        index = InvertedIndex.fromPostings(results)
//        println("Index size ${index.tokens.size}")
    }
    println("Took $time milliseconds (${time.toDouble() / 1000.0} seconds")
}

private fun index2(shardId: Int, docs: Collection<Doc>): Path {
    val index = HashMap<String, BinPosting>()
    val tokenizer = BetterTrigramTokenizer2()
    val tokenPositions = HashMap<String, MutableList<Int>>()
    for (doc in docs) {
        tokenPositions.clear()
        for (token in tokenizer.tokenize(doc.path)) {
            tokenPositions.computeIfAbsent(token.lexeme) { ArrayList() }.add(token.position)
        }
        for ((token, positions) in tokenPositions) {
            val posting = index.computeIfAbsent(token) { BinPosting(token) }
            posting.put(doc.id, positions)
        }
    }

    val flushPath = Paths.get("/home/ceronman/indexes/$shardId.bin")
    Files.createFile(flushPath)
    val tokens = index.keys.sorted()
    Files.newByteChannel(flushPath, StandardOpenOption.WRITE).use { channel ->
        channel.write(ByteBuffer.allocate(4).putInt(tokens.size))
        for (t in tokens) {
            val posting = index[t]!!.close()
            channel.write(posting)
        }
    }

}

private fun index3(shardId: Int, docs: Collection<Doc>) {
    val shardSize = 100_000
    val tokenizer = BetterTrigramTokenizer2()
    val tokenPositions = HashMap<String, MutableList<Int>>()
    val postings = ArrayList<BinPosting>(shardSize)
    var c = 0
    for (doc in docs) {
        tokenPositions.clear()
        for (token in tokenizer.tokenize(doc.path)) {
            tokenPositions.computeIfAbsent(token.lexeme) { ArrayList() }.add(token.position)
        }
        for ((token, positions) in tokenPositions) {
            val posting  = BinPosting(token)
            posting.put(doc.id, positions)
            postings.add(BinPosting(token))
            if (postings.size == shardSize) {
                postings.sortBy { it.token }
                val bufferSize = postings.map { it.close().limit() }.sum() + 4
                val flushPath = Paths.get("/home/ceronman/indexes/${shardId}_$c.bin")
                Files.createFile(flushPath)
                val buffer = ByteBuffer.allocate(bufferSize)
                buffer.putInt(postings.size)
                for (posting in postings) {
                    buffer.put(posting.close())
                }
                Files.newByteChannel(flushPath, StandardOpenOption.WRITE).use { channel ->
                    channel.write(buffer)
                }
                postings.clear()
                c++
            }
        }
    }
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

class BinPosting(val token: String) {
    private var buffer = ByteBuffer.allocate(1024)
    private var numDocsOffset: Int = 0
    private var numDocs = 0
    init {
        val tokenBytes = token.toByteArray(Charsets.UTF_8)
        buffer.putInt(tokenBytes.size)
        buffer.put(tokenBytes)
        buffer.putInt(numDocs)
    }

    // Temporal
    fun capacity() = buffer.capacity()
    fun remaining() = buffer.remaining()
    fun close(): ByteBuffer {
        buffer.flip()
        return buffer.asReadOnlyBuffer()
    }

    fun put(docId: Int, positions: List<Int>) {
        growIfNeeded(Int.SIZE_BYTES * 2)
        buffer.putInt(docId)
        buffer.putInt(positions.size)
        numDocs++
        buffer.putInt(numDocsOffset, numDocs)
        var prev = 0
        for (position in positions) {
            putVarInt(position - prev)
            prev = position
        }
    }

    private fun putVarInt(value: Int) {
        growIfNeeded(5) // Worst case scenario a VarInt requires 5 bytes
        var v = value
        while (true) {
            val bits = v and 0x7f
            v = v ushr 7
            if (v == 0) {
                buffer.put(bits.toByte())
                return
            }
            buffer.put((bits or 0x80).toByte())
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