package com.ceronman.jetindexer

interface QueryResolver {
    fun search(term: String): List<QueryResult>
}

class StandardQueryResolver(private val index: TokenIndex): QueryResolver {
    override fun search(term: String): List<QueryResult> {
        val view = index.rawQuery(term)
        return view.readPostings()
                .filter { index.documentPath(it.docId) != null }
                .map { QueryResult(term, index.documentPath(it.docId)!!, it.position) }
                .toList()
    }
}

class TrigramSubstringQueryResolver(private val index: TokenIndex): QueryResolver {
    private val tokenizer = TriTokenizer()

    override fun search(term: String): List<QueryResult> {
        val postings = TriTokenizer().tokenize(term).map { index.rawQuery(it.lexeme) }.toList()

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