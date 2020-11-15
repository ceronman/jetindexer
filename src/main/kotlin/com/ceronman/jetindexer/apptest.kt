package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.system.measureTimeMillis

private val log = LoggerFactory.getLogger("app")

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

    log.info("Indexing {}", path)

    val paths = FileWalker().walk(listOf(path))
    log.info("Found {} files", paths.size)

    val index = TokenIndex(paths)

    var time = measureTimeMillis {
        index.index()
    }
    log.info("Indexing took $time milliseconds (${time.toDouble() / 1000.0} seconds")

//    time = measureTimeMillis {
//        for (i in 1..100) {
//            index.search("ParameterTableModelItemBase")
//        }
//    }
//    log.info("100 queries took $time milliseconds ( ${time / 100} per search )")

    val result = index.search("ParameterTableModelItemBase")
    val resultsByFile = result.groupBy { it.path }
    for ((path, posting) in resultsByFile) {
        println(path)
        for (p in posting) {
            println(p.position)
        }
    }
}