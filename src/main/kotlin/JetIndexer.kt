import java.nio.file.Path

class JetIndexer(
        val tokenizer: Tokenizer,
        val paths: Collection<Path>
) {
    fun start(): Nothing = TODO()
    fun stop(): Nothing = TODO()
    fun query(term: String): List<QueryResult> = TODO()
}

data class QueryResult(val term: String,
                       val fileName: Path,
                       val position: Int)