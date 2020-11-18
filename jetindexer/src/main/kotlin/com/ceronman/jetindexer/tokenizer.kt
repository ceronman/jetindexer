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
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

/*
Represents a Token, which is simply a string and its position in a file.
 */
data class Token(
    val text: String,
    val position: Int
)

/**
 * Splits a document text in multiple tokens. This can be implemented using multiple strategies.
 *
 * The Tokenizer responsibility is to read a given file and produce a sequence of tokens.
 */
interface Tokenizer {
    fun tokenize(path: Path): Sequence<Token>
}

/**
 * A simple implementation of [Tokenizer] that just splits the text by any whitespace.
 *
 * This can be used by simple plain text. A more complete implementation should take
 * punctuation marks into account.
 */
class WhiteSpaceTokenizer : Tokenizer {
    override fun tokenize(path: Path): Sequence<Token> = sequence {
        val bytes = Files.readAllBytes(path)
        val string = String(bytes, Charsets.UTF_8)
        val token = StringBuilder()
        var position = 0
        for (c in string) {
            if (c.isWhitespace()) {
                if (token.isNotEmpty()) {
                    if (token.length < 256) {
                        yield(Token(token.toString(), position - token.length))
                    }
                    token.clear()
                }
            } else {
                token.append(c)
            }
            position++
        }
        if (token.isNotEmpty()) {
            if (token.length < 256) {
                yield(Token(token.toString(), position - token.length))
            }
        }
    }
}

/**
 * An implementation of [Tokenizer] that creates trigram tokens for a given file.
 *
 * For example, if a file contains the word "kotlin". This would be tokenized as
 * ("kot", "otl", "tli", "lin").
 *
 * This allows to use a [QueryResolver] that can look for arbitrary substrings in
 * an index.
 *
 * Theory for this is explained here:
 *
 * https://swtch.com/~rsc/regexp/regexp4.html
 *
 * @see [TrigramSubstringQueryResolver]
 */
class TrigramTokenizer : Tokenizer {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun tokenize(path: Path): Sequence<Token> {
        val decoder = Charsets.UTF_8.newDecoder()
        val bytes = Files.readAllBytes(path)
        val string = try {
            decoder.decode(ByteBuffer.wrap(bytes)).toString()
        } catch (e: CharacterCodingException) {
            log.warn("File $path does not contain valid UTF-8 text")
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
        yield(Token(str.substring(start, end), start))
    }
}

/**
 * An implementation of [Tokenizer] that looks for patterns in a file
 * matching a regular expression and produces tokens that match that regex.
 *
 * For example, to tokenize by identifier of the Java programming language
 * it's possible to use the [a-zA-Z_$][a-zA-Z\\d_$]* pattern
 */
class RegexTokenizer(private val tokenPattern: Regex): Tokenizer {
    override fun tokenize(path: Path): Sequence<Token> {
        val bytes = Files.readAllBytes(path)
        val string = String(bytes, Charsets.UTF_8)
        val results = tokenPattern.findAll(string)
        return results.map { Token(string.substring(it.range), it.range.first) }
    }
}