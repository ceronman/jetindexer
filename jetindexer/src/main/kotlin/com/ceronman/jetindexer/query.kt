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

import java.nio.file.Path

/**
 * Represent a result of doing a search on an index.
 */
data class QueryResult(
    val term: String,
    val path: Path,
    val position: Int
)

/**
 * Searches an inverted index with a given query.
 *
 * Implementations of this can use different strategies to translate the provided
 * query into a series of raw queries to the inverted index. This allows to customize
 * how the search id done.
 */
interface QueryResolver {
    fun search(index: InvertedIndex, query: String): Sequence<QueryResult>
}

/**
 * Standard implementation of [QueryResolver] that simply assumes that the query
 * is just a simple token. And then performs a single query to the inverted index.
 */
class StandardQueryResolver() : QueryResolver {
    override fun search(index: InvertedIndex, query: String): Sequence<QueryResult> {
        val view = index.rawQuery(query)
        return view.readPostings()
            .filter { index.documentPath(it.docId) != null }
            .map { QueryResult(query, index.documentPath(it.docId)!!, it.position) }
    }
}

/**
 * An implementation of [QueryResolver] that uses a trigram index to search for a
 * string in the index. @see [TrigramTokenizer]
 *
 * This expects the index to be generated using a [TrigramTokenizer] which splits
 * documents in trigrams, e.g.
 *
 * "kotlin" -> ("kot", "otl", "tli", "lin")
 *
 * Then when looking for a given term in the index, the term is again split in trigrams
 * in the same way and the inverted index is queried for each of those trigrams.
 *
 * The results of those searches are merged taking checking that the positions
 * of the trigrams have the correct files and positions.
 * e.g "kot" -> p, "otl" -> p + 1,...
 *
 * More details about this:
 *
 * https://swtch.com/~rsc/regexp/regexp4.html
 */
class TrigramSubstringQueryResolver() : QueryResolver {
    private val tokenizer = TrigramTokenizer()

    override fun search(index: InvertedIndex, query: String): Sequence<QueryResult> {
        val postings = tokenizer.tokenize(query).map { index.rawQuery(it.text) }.toList()

        if (postings.isEmpty()) {
            return emptySequence()
        }

        return mergePostings(postings)
            .filter { posting -> index.documentPath(posting.docId) != null }
            .map { posting -> QueryResult(query, index.documentPath(posting.docId)!!, posting.position) }
    }

    private fun mergePostings(postings: List<PostingListView>): Sequence<Posting> = sequence {
        val numLists = postings.size
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
                yield(firstPosting)
            }
        }
    }

}