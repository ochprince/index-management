/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import com.colasoft.opensearch.action.ActionResponse
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.ToXContentObject
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_TYPE
import com.colasoft.opensearch.indexmanagement.util._ID
import com.colasoft.opensearch.indexmanagement.util._PRIMARY_TERM
import com.colasoft.opensearch.indexmanagement.util._SEQ_NO
import com.colasoft.opensearch.indexmanagement.util._VERSION
import com.colasoft.opensearch.rest.RestStatus

class IndexSMPolicyResponse(
    val id: String,
    val version: Long,
    val seqNo: Long,
    val primaryTerm: Long,
    val policy: SMPolicy,
    val status: RestStatus
) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        policy = SMPolicy(sin),
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        policy.writeTo(out)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_SEQ_NO, seqNo)
            .field(_PRIMARY_TERM, primaryTerm)
            .field(SM_TYPE, policy, XCONTENT_WITHOUT_TYPE_AND_USER)
            .endObject()
    }
}
