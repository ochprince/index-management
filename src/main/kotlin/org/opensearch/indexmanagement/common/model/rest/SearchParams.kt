/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The ColaSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.common.model.rest

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.search.sort.FieldSortBuilder
import com.colasoft.opensearch.search.sort.SortBuilders
import com.colasoft.opensearch.search.sort.SortOrder
import java.io.IOException

const val DEFAULT_PAGINATION_SIZE = 20
const val DEFAULT_PAGINATION_FROM = 0
const val DEFAULT_SORT_ORDER = "asc"
const val DEFAULT_QUERY_STRING = "*"

data class SearchParams(
    val size: Int,
    val from: Int,
    val sortField: String,
    val sortOrder: String,
    val queryString: String
) : Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        size = sin.readInt(),
        from = sin.readInt(),
        sortField = sin.readString(),
        sortOrder = sin.readString(),
        queryString = sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeInt(size)
        out.writeInt(from)
        out.writeString(sortField)
        out.writeString(sortOrder)
        out.writeString(queryString)
    }

    fun getSortBuilder(): FieldSortBuilder {
        return SortBuilders
            .fieldSort(this.sortField)
            .order(SortOrder.fromString(this.sortOrder))
    }
}
