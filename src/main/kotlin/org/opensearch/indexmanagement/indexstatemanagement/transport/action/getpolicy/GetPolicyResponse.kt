/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.colasoft.opensearch.action.ActionResponse
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.ToXContentObject
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.util._ID
import com.colasoft.opensearch.indexmanagement.util._PRIMARY_TERM
import com.colasoft.opensearch.indexmanagement.util._SEQ_NO
import com.colasoft.opensearch.indexmanagement.util._VERSION
import java.io.IOException

class GetPolicyResponse : ActionResponse, ToXContentObject {

    val id: String
    val version: Long
    val seqNo: Long
    val primaryTerm: Long
    val policy: Policy?

    constructor(
        id: String,
        version: Long,
        seqNo: Long,
        primaryTerm: Long,
        policy: Policy?
    ) : super() {
        this.id = id
        this.version = version
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.policy = policy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        policy = sin.readOptionalWriteable(::Policy)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeOptionalWriteable(policy)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_SEQ_NO, seqNo)
            .field(_PRIMARY_TERM, primaryTerm)
        if (policy != null) {
            val policyParams = ToXContent.MapParams(mapOf(WITH_TYPE to "false", WITH_USER to "false", Action.EXCLUDE_CUSTOM_FIELD_PARAM to "true"))
            builder.field(Policy.POLICY_TYPE, policy, policyParams)
        }

        return builder.endObject()
    }
}
