/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.delete

import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.action.delete.DeleteRequest
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput

class DeleteSMPolicyRequest : DeleteRequest {
    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(sin: StreamInput) : super(sin)

    constructor(id: String) {
        super.id(id)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
    }
}
