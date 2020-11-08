package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.system.measureTimeMillis

private val tokenizer = TrigramTokenizer()
//private val index = ConcurrentHashMap<String, MutableList<Int>>()
private val log = LoggerFactory.getLogger("app")

fun main(args: Array<String>) = runBlocking {
//    val path = Paths.get("/home/ceronman/code/github/intellij-community")
    val path = Paths.get("/home/ceronman/code/github/linux")

//    val bf = ByteBuffer.allocate(1000)
//    println("pos: ${bf.position()}, limit: ${bf.limit()}, cap: ${bf.capacity()}, rem: ${bf.remaining()}")
//    bf.put("hello world".toByteArray())
//    println("pos: ${bf.position()}, limit: ${bf.limit()}, cap: ${bf.capacity()}, rem: ${bf.remaining()}")
//    bf.flip()
//    println("pos: ${bf.position()}, limit: ${bf.limit()}, cap: ${bf.capacity()}, rem: ${bf.remaining()}")
//    val bytes = ByteArray(bf.limit())
//    bf.get(bytes)
//    println("result: ${String(bytes)}")
//    System.exit(1)

    println("Indexing $path")
    val time = measureTimeMillis {
        val paths = walkPaths(path)
        println("Got ${paths.size} paths")
//        val chunkSize = paths.size / 4
        val chunkSize = 100
        val chunks= paths.chunked(chunkSize)
        println("Using ${chunks.size} chunks of size $chunkSize")

//        val jobs = ArrayList<Job>()
//        for ((id, paths) in paths.chunked(chunkSize).withIndex()) {
//            val job = launch(Dispatchers.Default) {
//                index2(id, paths)
//            }
//            jobs.add(job)
//        }
//        jobs.joinAll()

//        val jobs = ArrayList<Deferred<Path>>()
//        for ((id, paths) in paths.chunked(chunkSize).withIndex()) {
//            val job = async(Dispatchers.Default) {
//                index2(id, paths)
//            }
//            jobs.add(job)
//        }
//        val results = jobs.awaitAll()

//        val finalIndex = HashMap<String, ArrayList<Int>>()
//        val l = ArrayList<ByteArray>()
//        for (p in results) {
//            l.add(Files.readAllBytes(p))
//        }

        val jobs = ArrayList<Deferred<ByteArray>>()
        for ((id, paths) in paths.chunked(chunkSize).withIndex()) {
            val job = async(Dispatchers.Default) {
                index3(id, paths)
            }
            jobs.add(job)
        }
        val results = jobs.awaitAll()
        println("Jobs done")

        val result = mergeChunks(results)
        println("Size: ${result.size}")

//        val allTokens = HashSet<String>()
//        for (bytes in results) {
//            val buffer = ByteBuffer.wrap(bytes)
//            while (buffer.hasRemaining()) {
//                val tokenSize = buffer.int
//                val tokenBytes = ByteArray(tokenSize)
//                buffer.get(tokenBytes)
//                val numDocs = buffer.int
//                allTokens.add(String(tokenBytes))
//                for (i in 0 until numDocs) {
//                    buffer.int
//                }
//            }
//        }
//        println("Size: ${allTokens.size}")

//        val merged = HashMap<String, MutableList<Int>>()
//        for (bytes in results) {
//            val buffer = ByteBuffer.wrap(bytes)
//            var tokenCount = 0
//            while (buffer.hasRemaining()) {
//                val tokenSize = buffer.int
//                val tokenBytes = ByteArray(tokenSize)
//                buffer.get(tokenBytes)
//                val numDocs = buffer.int
//                val docs = merged.computeIfAbsent(String(tokenBytes)) { ArrayList() }
//                for (i in 0 until numDocs) {
//                    docs.add(buffer.int)
//                }
//                tokenCount++
//            }
//        }

//        val jobs = ArrayList<Deferred<Map<String, List<Int>>>>()
//        for ((id, paths) in paths.chunked(chunkSize).withIndex()) {
//            val job = async(Dispatchers.Default) {
//                index4(id, paths)
//            }
//            jobs.add(job)
//        }
//        val results = jobs.awaitAll()
//        println("Jobs done")
//        val merged = HashMap<String, MutableList<Int>>()
//        for (result in results) {
//            for ((token, documents) in result) {
//                val docs = merged.computeIfAbsent(token) { ArrayList() }
//                docs.addAll(documents)
//            }
//        }

//        val index = index4(1, paths)
//        val max = index.values.map { it.size }.max()
//        println("Size: ${index.size}, max: $max")
    }

    println("Took $time milliseconds (${time.toDouble() / 1000.0} seconds)")
//    delay(1000000)
}

private fun index(id: Int, paths: Collection<Path>)  {
    val capacity = 1_000_000
    val index = ArrayList<String>(capacity)
    var flushCount = 0
    var docId = 0
    for (path in paths) {
        docId++
        val bytes = Files.readAllBytes(path)
        val string = String(bytes, Charsets.UTF_8)
        val tokens = HashSet<String>()
        for (i in 2 until string.length - 3) {
            val x = string[i]
            val token = string.substring(i, i + 3)
            tokens.add(token)
        }

        for (token in tokens) {
            if (index.size < capacity) {
                index.add(token)
            } else {

                index.sort()
                val flushPath = Paths.get("/home/ceronman/indexes/posting${id}_${flushCount}.bin")
                Files.createFile(flushPath)
                Files.newByteChannel(flushPath, StandardOpenOption.WRITE).use { channel ->
                    val buffer = ByteBuffer.allocate(50_000_000)
                    for (token in index) {
                        val tokenBytes = token.toByteArray()
                        if (buffer.remaining() > (tokenBytes.size + Int.SIZE_BYTES) + 8) {
                            buffer.put(tokenBytes)
                            buffer.putInt(docId)
                        } else {
                            println("Capacity exceeded!")
                        }
                    }
                    buffer.flip()
                    channel.write(buffer)
                }
                index.clear()
                flushCount++
            }
        }
    }
}

private fun index2(id: Int, paths: Collection<Path>): Path {
    val index = HashMap<String, MutableSet<Int>>()
    for ((docId, path) in paths.withIndex()) {
        val bytes = Files.readAllBytes(path)
        val string = String(bytes, Charsets.UTF_8)
        val tokens = HashSet<String>()
        for (i in 2 until string.length - 3) {
            val token = string.substring(i, i + 3)
            tokens.add(token)
        }

        for (token in tokens) {
            index.computeIfAbsent(token){ HashSet() }.add(docId)
        }
    }

    val flushPath = Paths.get("/home/ceronman/indexes/posting_${id}.bin")
    Files.deleteIfExists(flushPath)
    Files.createFile(flushPath)
    Files.newByteChannel(flushPath, StandardOpenOption.WRITE).use { channel ->
        val buffer = ByteBuffer.allocate(50_000_000)
        for (token in index.keys.sorted()) {
            val tokenBytes = token.toByteArray()
            val docIds = index[token]!!
            if (buffer.remaining() > (8 + tokenBytes.size + Int.SIZE_BYTES * docIds.size)) {
                buffer.putInt(tokenBytes.size)
                buffer.put(tokenBytes)
                buffer.putInt(docIds.size)
                for (docId in docIds.sorted()) {
                    buffer.putInt(docId)
                }
            } else {
                println("Capacity exceeded!")
            }
        }
        buffer.flip()
        channel.write(buffer)
    }
    return flushPath

//    ByteArrayOutputStream().use { out ->
//        for (token in index.keys.sorted()) {
//            out.write(token.toByteArray())
//            for (docId in index[token]!!) {
//                out.write(ByteBuffer.allocate(4).putInt(docId).array())
//            }
//        }
//        return out.toByteArray()
//    }
//    val buffer = ByteBuffer.allocate(5_000_000)
//    for (token in index.keys.sorted()) {
//        val tokenBytes = token.toByteArray()
//        if (buffer.remaining() > tokenBytes.size) {
//            buffer.put(tokenBytes)
//            for (docId in index[token]!!) {
//                buffer.putInt(docId)
//            }
//        } else {
//            println("No capacity!")
//        }
//    }
//    println("Buffer size: ${buffer.position()}")
//    println("Index size: ${index.size} for ${paths.size} paths")
}

private fun index3(id: Int, paths: Collection<Path>): ByteArray {
    val index = HashMap<String, MutableList<Int>>()
    for ((docId, path) in paths.withIndex()) {
        val bytes = Files.readAllBytes(path)
        val string = String(bytes, Charsets.UTF_8)
        val tokens = HashSet<String>()
        for (i in 2 until string.length - 3) {
            val token = string.substring(i, i + 3)
            tokens.add(token)
        }

        for (token in tokens) {
            index.computeIfAbsent(token){ ArrayList() }.add(docId)
        }
    }
    val buffer = ByteBuffer.allocate(50_000_000)
    for (token in index.keys.sorted()) {
        val docIds = index[token]!!
        val tokenBytes = token.toByteArray()
        if (buffer.remaining() > (8 + tokenBytes.size + Int.SIZE_BYTES * docIds.size)) {
            buffer.putInt(tokenBytes.size)
            buffer.put(tokenBytes)
            buffer.putInt(docIds.size)
            for (docId in docIds.sorted()) {
                buffer.putInt(docId)
            }
        } else {
            println("Capacity exceeded!")
        }
    }
    buffer.flip()
    val result = ByteArray(buffer.limit())
    buffer.get(result)
    return result
}

private fun index4(id: Int, paths: Collection<Path>): Map<String, List<Int>> {
    val index = HashMap<String, MutableList<Int>>()
    for ((docId, path) in paths.withIndex()) {
        val bytes = Files.readAllBytes(path)
        val string = String(bytes, Charsets.UTF_8)
        val tokens = HashSet<String>()
        for (i in 2 until string.length - 3) {
            val token = string.substring(i, i + 3)
            tokens.add(token)
        }

        for (token in tokens) {
            index.computeIfAbsent(token){ ArrayList() }.add(docId)
        }
    }
   return index
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

private fun mergeChunks(chunks: Collection<ByteArray>): ByteArray {
    val outBuffer = ByteBuffer.allocate(500_000_000)
    val buffers = chunks.map { ByteBuffer.wrap(it) }
    val postings = buffers.map { getPosting(it) }.toMutableList()
    while (postings.any { it != null}) {
        val token = postings.filterNotNull().minByOrNull { it.token }!!.token
        val docIds = ArrayList<Int>()
        for (i in 0 until postings.size) {
            val posting = postings[i] ?: continue

            if (posting.token == token) {
                docIds.addAll(posting.documents)
                postings[i] = getPosting(buffers[i])
            }
        }
        setPosting(Posting(token, docIds), outBuffer)
    }
    outBuffer.flip()
    val result = ByteArray(outBuffer.limit())
    outBuffer.get(result)
    return result
}

data class Posting(val token: String, val documents: List<Int>)

private fun getPosting(buffer: ByteBuffer): Posting?
{
    if (buffer.hasRemaining()) {
        val tokenSize = buffer.int
        val tokenBytes = ByteArray(tokenSize)
        buffer.get(tokenBytes)
        val numDocs = buffer.int
        val docs = ArrayList<Int>()
        for (i in 0 until numDocs) {
            docs.add(buffer.int)
        }
        return Posting(String(tokenBytes), docs)
    } else {
        return null
    }
}

private fun setPosting(posting: Posting, buffer: ByteBuffer) {
    val tokenBytes = posting.token.toByteArray()
    val docIds = posting.documents
    if (buffer.remaining() > (8 + tokenBytes.size + Int.SIZE_BYTES * docIds.size)) {
        buffer.putInt(tokenBytes.size)
        buffer.put(tokenBytes)
        buffer.putInt(docIds.size)
        for (docId in docIds.sorted()) {
            buffer.putInt(docId)
        }
    } else {
        println("Write Capacity exceeded!")
    }
}
