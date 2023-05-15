/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import com.colasoft.opensearch.action.ActionRequest
import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput

class GetSMPolicyRequest(
    val policyID: String
) : ActionRequest() {
    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(sin: StreamInput) : this(
        policyID = sin.readString(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(policyID)
    }
}
