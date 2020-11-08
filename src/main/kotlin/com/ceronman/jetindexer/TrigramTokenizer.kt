package com.ceronman.jetindexer

class TrigramTokenizer : Tokenizer {
    override fun tokenize(input: Sequence<Char>): Sequence<Token> = sequence {
        val builder = StringBuilder(3)
        val iterator = input.iterator()
        for (i in 0..2) {
            if (!iterator.hasNext()) {
                return@sequence
            }
            builder.append(iterator.next())
        }
        var position = 0
        yield(Token(builder.toString(), position))
        while (iterator.hasNext()) {
            position++
            builder[0] = builder[1]
            builder[1] = builder[2]
            builder[2] = iterator.next()
            yield(Token(builder.toString(), position))
        }
    }
}