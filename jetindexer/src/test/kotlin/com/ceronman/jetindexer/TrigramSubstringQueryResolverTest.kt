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

internal class TrigramSubstringQueryResolverTest {

    @Test
    fun search(@TempDir tempDir: Path) {
        val index = InvertedIndex(TrigramTokenizer())
        val path1 = writeFile(tempDir, "six sixteen sixty")
        val path2 = writeFile(tempDir, "-----six----")
        val path3 = writeFile(tempDir, "And sssssix")
        val path4 = writeFile(tempDir, "Other text")

        index.addBatch(listOf(path1, path2, path3, path4))

        val results3 = TrigramSubstringQueryResolver().search(index, "six")

        kotlin.test.assertEquals(
            listOf(
                QueryResult("six", path1, 0),
                QueryResult("six", path1, 4),
                QueryResult("six", path1, 12),
                QueryResult("six", path2, 5),
                QueryResult("six", path3, 8),
            ),
            results3.toList()
        )
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir.toRealPath(), "test", ".txt")
        path.toFile().writeText(contents)
        return path
    }
}