package com.ceronman.jetindexer

import java.nio.file.Path

data class QueryResult(
    val term: String,
    val path: Path,
    val position: Int
)

interface QueryResolver {
    fun search(index: TokenIndex, term: String): List<QueryResult>
}

class StandardQueryResolver() : QueryResolver {
    override fun search(index: TokenIndex, term: String): List<QueryResult> {
        val view = index.rawQuery(term)
        return view.readPostings()
            .filter { index.documentPath(it.docId) != null }
            .map { QueryResult(term, index.documentPath(it.docId)!!, it.position) }
            .toList()
    }
}

class TrigramSubstringQueryResolver() : QueryResolver {
    private val tokenizer = TrigramTokenizer()

    override fun search(index: TokenIndex, term: String): List<QueryResult> {
        val postings = tokenizer.tokenize(term).map { index.rawQuery(it.text) }.toList()

        if (postings.isEmpty()) {
            return emptyList()
        }

        return mergePostings(postings)
            .filter { posting -> index.documentPath(posting.docId) != null }
            .map { posting -> QueryResult(term, index.documentPath(posting.docId)!!, posting.position) }
    }

    private fun mergePostings(postings: List<PostingListView>): List<Posting> {
        val numLists = postings.size
        val result = ArrayList<Posting>()
        val iterators = postings.map { it.peekableIterator() }
        while (iterators.all { it.hasNext() }) {
            val firstPosting = iterators[0].peek()
            var equals = true
            for (i in 1 until numLists) {
                val currentPosting = iterators[i].peek()
                if (firstPosting.docId < currentPosting.docId) {
                    while (iterators[0].hasNext() && iterators[0].peek().docId < currentPosting.docId) {
                        iterators[0].next()
                    }
                } else if (currentPosting.docId < firstPosting.docId) {
                    while (iterators[i].hasNext() && iterators[i].peek().docId < firstPosting.docId) {
                        iterators[i].next()
                    }
                } else if (firstPosting.position < currentPosting.position - i) {
                    iterators[0].next()
                } else if (firstPosting.position > currentPosting.position - i) {
                    iterators[i].next()
                } else {
                    continue
                }
                equals = false
                break
            }
            if (equals) {
                iterators.forEach { it.next() }
                result.add(firstPosting)
            }
        }
        return result
    }

}