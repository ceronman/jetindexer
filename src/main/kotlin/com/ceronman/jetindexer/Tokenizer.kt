package com.ceronman.jetindexer

interface Tokenizer {
    fun tokenize(input: Sequence<Char>): Sequence<Token>
}