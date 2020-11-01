package com.ceronman.jetindexer

import java.nio.file.*

data class QueryResult(
    val term: String,
    val path: Path,
    val position: Int
)