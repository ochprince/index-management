/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action

import com.colasoft.opensearch.action.ActionResponse
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.ToXContentObject
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.buildInvalidIndexResponse
import java.io.IOException

open class ISMStatusResponse : ActionResponse, ToXContentObject {

    val updated: Int
    val failedIndices: List<FailedIndex>

    constructor(
        updated: Int,
        failedIndices: List<FailedIndex>
    ) : super() {
        this.updated = updated
        this.failedIndices = failedIndices
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        updated = sin.readInt(),
        failedIndices = sin.readList(::FailedIndex)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeInt(updated)
        out.writeCollection(failedIndices)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(UPDATED_INDICES, updated)
        buildInvalidIndexResponse(builder, failedIndices)
        return builder.endObject()
    }
}
