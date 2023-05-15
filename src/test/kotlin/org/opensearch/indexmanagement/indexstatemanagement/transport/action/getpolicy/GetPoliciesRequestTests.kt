/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.indexmanagement.common.model.rest.SearchParams
import com.colasoft.opensearch.test.OpenSearchTestCase

class GetPoliciesRequestTests : OpenSearchTestCase() {

    fun `test get policies request`() {
        val table = SearchParams(20, 0, "policy.policy_id.keyword", "desc", "*")
        val req = GetPoliciesRequest(table)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetPoliciesRequest(sin)
        assertEquals(table, newReq.searchParams)
    }
}
