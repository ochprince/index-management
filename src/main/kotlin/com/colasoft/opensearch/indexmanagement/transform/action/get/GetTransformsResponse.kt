/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.get

import com.colasoft.opensearch.action.ActionResponse
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.xcontent.ToXContent
import com.colasoft.opensearch.common.xcontent.ToXContentObject
import com.colasoft.opensearch.common.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.indexmanagement.transform.model.Transform.Companion.TRANSFORM_TYPE
import com.colasoft.opensearch.indexmanagement.util._ID
import com.colasoft.opensearch.indexmanagement.util._PRIMARY_TERM
import com.colasoft.opensearch.indexmanagement.util._SEQ_NO
import com.colasoft.opensearch.rest.RestStatus
import java.io.IOException

class GetTransformsResponse(
    val transforms: List<Transform>,
    val totalTransforms: Int,
    val status: RestStatus
) : ActionResponse(), ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        transforms = sin.readList(::Transform),
        totalTransforms = sin.readInt(),
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(transforms)
        out.writeInt(totalTransforms)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("total_transforms", totalTransforms)
            .startArray("transforms")
            .apply {
                for (transform in transforms) {
                    this.startObject()
                        .field(_ID, transform.id)
                        .field(_SEQ_NO, transform.seqNo)
                        .field(_PRIMARY_TERM, transform.primaryTerm)
                        .field(TRANSFORM_TYPE, transform, XCONTENT_WITHOUT_TYPE_AND_USER)
                        .endObject()
                }
            }
            .endArray()
            .endObject()
    }
}
