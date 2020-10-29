interface Tokenizer {
    fun tokenize(input: Sequence<Char>): Sequence<Token>
}

data class Token(
    val lexeme: String,
    val position: Int
)

class WhiteSpaceTokenizer : Tokenizer {
    override fun tokenize(input: Sequence<Char>): Sequence<Token> = sequence {
        val token = StringBuilder()
        var position = 0
        for (c in input) {
            if (c.isWhitespace()) {
                if (token.isNotEmpty()) {
                    yield(Token(token.toString(), position - token.length))
                    token.clear()
                }
            } else {
                token.append(c)
            }
            position++
        }
        if (token.isNotEmpty()) {
            yield(Token(token.toString(), position - token.length))
        }
    }
}