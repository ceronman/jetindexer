package com.ceronman.jetindexer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import kotlin.system.measureTimeMillis

private val log = LoggerFactory.getLogger("app")

fun main() = runBlocking {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    val path = Paths.get("/Users/mceron/git_tree/github/intellij-community")
//    val path = Paths.get("/home/ceronman/code/github/linux")
//    val path = Paths.get("/home/ceronman/code/loxido")
//    val path = Paths.get("/home/ceronman/problems")

    log.info("Indexing {}", path)

    val paths = FileWalker().walk(listOf(path))
    log.info("Found {} files", paths.size)

//    val index = TokenIndex(WhiteSpaceTokenizer())
    val index = TokenIndex(TrigramTokenizer())

    var time = measureTimeMillis {
        index.addBatch(paths)
    }
    log.info("Indexing took $time milliseconds (${time.toDouble() / 1000.0} seconds")

    time = measureTimeMillis {
        val updatePaths = paths.slice(0..10_000)
        for (p in updatePaths) {
            index.update(p)
        }
    }
    log.info("Updating 10,000 documents took ${time / 1000} seconds")

    val queryResolver = TrigramSubstringQueryResolver()
//    val queryResolver = StandardQueryResolver()

    time = measureTimeMillis {
        for (i in 1..100) {
            queryResolver.search(index, "ParameterTableModelItemBase")
        }
    }
    log.info("100 queries took $time milliseconds ( ${time / 100} per search )")

    log.info("Running queries in parallel")
    val expected = queryResolver.search(index, "ParameterTableModelItemBase")
    for (i in 0 until 100) {
        launch(Dispatchers.Default) {
            val r = queryResolver.search(index, "ParameterTableModelItemBase")
            if (r != expected) {
                throw Exception("Parallel search failed")
            }
        }
    }

    val result = queryResolver.search(index, "ParameterTableModelItemBase")
    val resultsByFile = result.groupBy { it.path }
    log.info("Results of searching for 'ParameterTableModelItemBase'")
    for ((p, posting) in resultsByFile) {
        log.info("Found path: $p with positions:")
        for (r in posting) {
            log.info("${r.position}")
        }
    }

}