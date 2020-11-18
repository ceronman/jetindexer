package com.ceronman.jetindexer

import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

data class Token(
    val lexeme: String,
    val position: Int
)

// TODO: Handle tokenization errors
interface Tokenizer {
    fun tokenize(path: Path): Sequence<Token>
}

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