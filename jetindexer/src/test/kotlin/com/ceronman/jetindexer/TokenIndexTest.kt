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

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class TokenIndexTest {

    @Test
    internal fun addBatch(@TempDir tempDir: Path) {
        val index = InvertedIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "one two three four")
        val path2 = writeFile(tempDir, "two four six eight")
        val path3 = writeFile(tempDir, "two four eight sixteen")

        index.addBatch(listOf(path1, path2, path3)) { progress ->
            assertTrue { progress in 0..100 }
        }

        val results3 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 0),
            ),
            results3.toList()
        )
    }

    @Test
    internal fun add(@TempDir tempDir: Path) {
        val index = InvertedIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "one two three four")
        val path2 = writeFile(tempDir, "two four six eight")
        val path3 = writeFile(tempDir, "two four eight sixteen")
        index.add(path1)

        val results1 = StandardQueryResolver().search(index, "one")

        assertEquals(
            listOf(QueryResult("one", path1, 0)),
            results1.toList()
        )

        index.add(path2)
        val results2 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
            ),
            results2.toList()
        )

        index.add(path3)
        val results3 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 0),
            ),
            results3.toList()
        )
    }

    @Test
    internal fun delete(@TempDir tempDir: Path) {
        val index = InvertedIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "one two three four")
        val path2 = writeFile(tempDir, "two four six eight")
        val path3 = writeFile(tempDir, "two four eight sixteen")
        index.add(path1)
        index.add(path2)
        index.add(path3)

        val results1 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 0),
            ),
            results1.toList()
        )

        index.delete(path3)

        val results2 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
            ),
            results2.toList()
        )
    }

    @Test
    internal fun update(@TempDir tempDir: Path) {
        val index = InvertedIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "one two three four")
        val path2 = writeFile(tempDir, "two four six eight")
        val path3 = writeFile(tempDir, "two four eight sixteen")
        index.add(path1)
        index.add(path2)
        index.add(path3)

        val results1 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 0),
            ),
            results1.toList()
        )

        path3.toFile().writeText( "one two")
        index.update(path3)

        val results2 = StandardQueryResolver().search(index, "two")
        assertEquals(
            listOf(
                QueryResult("two", path1, 4),
                QueryResult("two", path2, 0),
                QueryResult("two", path3, 4),
            ),
            results2.toList()
        )
    }

    @Test
    internal fun fullScan(@TempDir tempDir: Path) {
        val index = InvertedIndex(WhiteSpaceTokenizer())
        val path1 = writeFile(tempDir, "six sixteen sixty")
        val path2 = writeFile(tempDir, "-----six----")
        val path3 = writeFile(tempDir, "And sssssix")
        val path4 = writeFile(tempDir, "Other text")
        index.addBatch(listOf(path1, path2, path3, path4))

        val results = FullScanQueryResolver().search(index, "si")
        assertEquals(
            listOf(
                QueryResult("si", path1, 0),
                QueryResult("si", path1, 4),
                QueryResult("si", path1, 12),
                QueryResult("si", path2, 5),
                QueryResult("si", path3, 8),
            ),
            results.toList()
        )
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir.toRealPath(), "test", ".txt")
        path.toFile().writeText(contents)
        return path
    }
}