package com.ceronman.jetindexer

import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import kotlin.collections.ArrayList

interface IndexingFilter {
    fun shouldIndex(path: Path): Boolean
}

class DefaultIndexingFilter: IndexingFilter {
    override fun shouldIndex(path: Path): Boolean {
        if (Files.isDirectory(path) && path.fileName.toString() == ".git") {
            return false
        }

        val contentType = Files.probeContentType(path)
        if (!contentType.contains("text")) {
            return false
        }

        if (Files.size(path) > 50_000_000) {
            return false
        }

        return true
    }
}

class FileWalker(private val filter: IndexingFilter = DefaultIndexingFilter()) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun walk(paths: Collection<Path>): List<Path> {
        val allPaths = ArrayList<Path>()
        for (path in paths) {
            Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
                    object : FileVisitor<Path> {
                        override fun preVisitDirectory(p: Path, attributes: BasicFileAttributes): FileVisitResult {
                            if (filter.shouldIndex(p)) {
                                log.debug("Skipping directory {} by indexing filter", p)
                                return FileVisitResult.SKIP_SUBTREE
                            } else {
                                log.debug("Scanning directory {}", p)
                                return FileVisitResult.CONTINUE
                            }
                        }

                        override fun visitFile(p: Path, attributes: BasicFileAttributes): FileVisitResult {
                            if (filter.shouldIndex(p)) {
                                log.debug("Found file {}", p)
                                allPaths.add(p)
                            } else {
                                log.debug("Skipping file {} by indexing filter", p)
                            }

                            return FileVisitResult.CONTINUE
                        }

                        override fun visitFileFailed(p: Path, e: IOException?): FileVisitResult {
                            log.warn("Unable to access file $p: $e")
                            log.debug("Exception raised", e)
                            return FileVisitResult.CONTINUE;
                        }

                        override fun postVisitDirectory(p: Path, e: IOException?): FileVisitResult {
                            if (e != null) {
                                log.warn("Error after accessing directory $p")
                                log.debug("Exception raised", e)
                            }
                            return FileVisitResult.CONTINUE
                        }
                    })
        }
        return allPaths
    }
}