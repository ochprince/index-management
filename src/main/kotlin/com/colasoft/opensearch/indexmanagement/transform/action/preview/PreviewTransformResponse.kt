/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.preview

import com.colasoft.opensearch.action.ActionResponse
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.xcontent.ToXContent
import com.colasoft.opensearch.common.xcontent.ToXContentObject
import com.colasoft.opensearch.common.xcontent.XContentBuilder
import com.colasoft.opensearch.rest.RestStatus

class PreviewTransformResponse(
    val documents: List<Map<String, Any>>,
    val status: RestStatus
) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        documents = sin.let {
            val documentList = mutableListOf<Map<String, Any>>()
            val size = it.readVInt()
            repeat(size) { _ ->
                documentList.add(sin.readMap()!!)
            }
            documentList.toList()
        },
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeVInt(documents.size)
        for (document in documents) {
            out.writeMap(document)
        }
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("documents", documents)
            .endObject()
    }
}
