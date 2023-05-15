/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.test.OpenSearchTestCase

class DeletePolicyRequestTests : OpenSearchTestCase() {

    fun `test delete policy request`() {
        val policyID = "policyID"
        val refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        val req = DeletePolicyRequest(policyID, refreshPolicy)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = DeletePolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(refreshPolicy, newReq.refreshPolicy)
    }
}
