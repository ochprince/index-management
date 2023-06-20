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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import com.colasoft.opensearch.action.ActionRequest
import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.action.ValidateActions
import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.util.NO_ID
import java.io.IOException

class IndexPolicyRequest : ActionRequest {

    val policyID: String
    val policy: Policy
    val seqNo: Long
    val primaryTerm: Long
    val refreshPolicy: WriteRequest.RefreshPolicy

    constructor(
        policyID: String,
        policy: Policy,
        seqNo: Long,
        primaryTerm: Long,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) : super() {
        this.policyID = policyID
        this.policy = policy
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.refreshPolicy = refreshPolicy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyID = sin.readString(),
        policy = Policy(sin),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        refreshPolicy = sin.readEnum(WriteRequest.RefreshPolicy::class.java)
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (NO_ID == policyID) {
            validationException = ValidateActions.addValidationError("Missing policyID", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(policyID)
        policy.writeTo(out)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeEnum(refreshPolicy)
    }
}
