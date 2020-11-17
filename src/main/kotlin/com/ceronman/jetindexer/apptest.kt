package com.ceronman.jetindexer

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.nio.file.*
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

    val index = TokenIndex(TrigramTokenizer())
//    val index = TokenIndex(WsTokenizer())

    var time = measureTimeMillis {
        index.addBatch(paths)
    }
    log.info("Indexing took $time milliseconds (${time.toDouble() / 1000.0} seconds")


//    val updatePaths = paths.slice(0..10000)
//    for (p in updatePaths) {
//        index.update(p)
//    }

//    time = measureTimeMillis {
//        for (i in 1..100) {
//            index.search("ParameterTableModelItemBase")
//        }
//    }
//    log.info("100 queries took $time milliseconds ( ${time / 100} per search )")

    val queryResolver = TrigramSubstringQueryResolver()
//    val queryResolver = StandardQueryResolver(index)

    for (i in 0 until 100) {
        launch(Dispatchers.Default) {
            queryResolver.search(index,"ParameterTableModelItemBase")
        }
    }

//    val result = queryResolver.search("public")
//    val result = queryResolver.search("ParameterTableModelItemBase")
//    val resultsByFile = result.groupBy { it.path }
//    for ((path, posting) in resultsByFile) {
//        println(path)
//        for (p in posting) {
//            println(p.position)
//        }
//    }

}