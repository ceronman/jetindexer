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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

internal class TrigramTokenizerTest {
    @Test
    internal fun simpleWordsString() {
        val tokenizer = TrigramTokenizer()
        assertEquals(
            listOf(
                Token("one", 0),
                Token("ne ", 1),
                Token("e t", 2),
                Token(" tw", 3),
                Token("two", 4),
                Token("wo ", 5),
                Token("o t", 6),
                Token(" th", 7),
                Token("thr", 8),
                Token("hre", 9),
                Token("ree", 10),
            ),
            tokenizer.tokenize("one two three").toList()
        )
    }

    @Test
    internal fun simpleWordsFromFile(@TempDir tempDir: Path) {
        val tokenizer = TrigramTokenizer()
        val path = writeFile(tempDir, "one two three")
        assertEquals(
            listOf(
                Token("one", 0),
                Token("ne ", 1),
                Token("e t", 2),
                Token(" tw", 3),
                Token("two", 4),
                Token("wo ", 5),
                Token("o t", 6),
                Token(" th", 7),
                Token("thr", 8),
                Token("hre", 9),
                Token("ree", 10),
            ),
            tokenizer.tokenize(path).toList()
        )
    }

    @Test
    internal fun smallWord(@TempDir tempDir: Path) {
        val tokenizer = TrigramTokenizer()
        val path = writeFile(tempDir, "on")
        assertEquals(
            emptyList<Token>(),
            tokenizer.tokenize(path).toList()
        )
    }

    private fun writeFile(dir: Path, contents: String): Path {
        val path = Files.createTempFile(dir, "test", "test")
        path.toFile().writeText(contents)
        return path
    }
}