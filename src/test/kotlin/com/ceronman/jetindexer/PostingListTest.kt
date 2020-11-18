package com.ceronman.jetindexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class PostingListTest {
    @Test
    internal fun singleList() {
        val postingList = PostingList()
        postingList.put(1, listOf(2, 4, 6, 8))
        postingList.put(2, listOf(5, 10, 15, 20))

        assertEquals(
            listOf(
                Posting(1, 2),
                Posting(1, 4),
                Posting(1, 6),
                Posting(1, 8),
                Posting(2, 5),
                Posting(2, 10),
                Posting(2, 15),
                Posting(2, 20),
            ), PostingListView(listOf(postingList.asReadOnly())).readPostings().toList()
        )
    }

    @Test
    internal fun multipleLists() {
        val postingList1 = PostingList()
        postingList1.put(1, listOf(2, 4, 6, 8))
        postingList1.put(2, listOf(5, 10, 15, 20))
        val postingList2 = PostingList()
        postingList2.put(3, listOf(100))
        postingList2.put(4, listOf(200))

        val postingListView = PostingListView(
            listOf(
                postingList1.asReadOnly(),
                postingList2.asReadOnly()
            )
        )

        assertEquals(
            listOf(
                Posting(1, 2),
                Posting(1, 4),
                Posting(1, 6),
                Posting(1, 8),
                Posting(2, 5),
                Posting(2, 10),
                Posting(2, 15),
                Posting(2, 20),
                Posting(3, 100),
                Posting(4, 200),
            ), postingListView.readPostings().toList()
        )
    }

    @Test
    internal fun emptyView() {
        val postingList = PostingList()
        postingList.put(1, listOf(2, 4, 6, 8))
        postingList.put(2, listOf(5, 10, 15, 20))

        assertEquals(
            emptyList<Posting>(),
            PostingListView(emptyList()).readPostings().toList()
        )
    }

    @Test
    internal fun iterator() {
        val postingList = PostingList()
        postingList.put(1, listOf(2, 4, 6, 8))
        postingList.put(2, listOf(5, 10, 15, 20))


        val postingListView = PostingListView(listOf(postingList.asReadOnly()))
        val result = ArrayList<Posting>()
        val i = postingListView.peekableIterator()
        assertEquals(Posting(1, 2), i.peek())
        while (i.hasNext()) {
            result.add(i.peek())
            i.next()
        }
        assertEquals(
            listOf(
                Posting(1, 2),
                Posting(1, 4),
                Posting(1, 6),
                Posting(1, 8),
                Posting(2, 5),
                Posting(2, 10),
                Posting(2, 15),
                Posting(2, 20),
            ), result
        )
    }
}