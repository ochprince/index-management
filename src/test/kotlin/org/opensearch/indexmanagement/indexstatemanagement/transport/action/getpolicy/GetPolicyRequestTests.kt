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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.search.fetch.subphase.FetchSourceContext
import com.colasoft.opensearch.test.OpenSearchTestCase

class GetPolicyRequestTests : OpenSearchTestCase() {

    fun `test get policy request`() {
        val policyID = "policyID"
        val version: Long = 123
        val fetchSrcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        val req = GetPolicyRequest(policyID, version, fetchSrcContext)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetPolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(version, newReq.version)
        assertEquals(fetchSrcContext, newReq.fetchSrcContext)
    }
}
