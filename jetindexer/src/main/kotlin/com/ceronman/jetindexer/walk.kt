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

import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import kotlin.collections.ArrayList

/**
 * Allows to filter which files should be indexed in a directory
 * when recursively walking it.
 */
interface IndexingFilter {
    fun shouldIndexFile(path: Path): Boolean
    fun shouldIndexDir(path: Path): Boolean
}

/**
 * A default implementation of [IndexingFilter] that just looks for extensions
 * an mime types that look like text files.
 *
 * This implementation could be improved considerably :(
 */
class DefaultIndexingFilter : IndexingFilter {
    private val goodExtensionsRegex = Regex(
        """
        \.(
            ant | bat | c | cgi | coffee | cpp | cs | d | fish | flake8 | flex | gradle |
            graphql | groovy | h | hpp | java | jql | js | json | jsp | jsx | kt | log | 
            php | py | sh | swift | tcl | txt | vb | xml | yaml | yml
        )$
    """.trimIndent(), RegexOption.COMMENTS
    )

    private val goodMimeTypes = Regex(
        """
        text | xml | json
    """.trimIndent(), RegexOption.COMMENTS
    )

    override fun shouldIndexFile(path: Path): Boolean {
        val contentType = Files.probeContentType(path)
        if (contentType == null && goodExtensionsRegex.find(path.fileName.toString()) == null) {
            return false
        }

        if (contentType != null && goodMimeTypes.find(contentType) == null) {
            return false
        }

        if (Files.size(path) > 50_000_000) {
            return false
        }

        return true
    }

    override fun shouldIndexDir(path: Path): Boolean {
        val name = path.fileName.toString()
        if (name.startsWith(".")) {
            return false
        }
        return true
    }
}

/**
 * Recursively walks a list of directories, filtering them by the provided [IndexingFilter]
 */
internal class FileWalker(private val filter: IndexingFilter = DefaultIndexingFilter()) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun walk(paths: Collection<Path>): List<Path> {
        val allPaths = ArrayList<Path>()
        for (path in paths) {
            Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
                object : FileVisitor<Path> {
                    override fun preVisitDirectory(p: Path, attributes: BasicFileAttributes): FileVisitResult {
                        if (filter.shouldIndexDir(p)) {
                            log.debug("Scanning directory {}", p)
                            return FileVisitResult.CONTINUE
                        } else {
                            log.debug("Skipping directory {} by indexing filter", p)
                            return FileVisitResult.SKIP_SUBTREE
                        }
                    }

                    override fun visitFile(p: Path, attributes: BasicFileAttributes): FileVisitResult {
                        if (filter.shouldIndexFile(p)) {
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
                        return FileVisitResult.CONTINUE
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