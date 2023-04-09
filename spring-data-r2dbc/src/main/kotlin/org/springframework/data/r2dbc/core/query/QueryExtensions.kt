package org.springframework.data.r2dbc.core.query

import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query

fun query(vararg args: Criteria?): Query = Query.query(
    args.fold(Criteria.empty()) { acc, arg ->
        arg?.let { acc.and(it) } ?: acc
    }
)
