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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.unit.TimeValue
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import com.colasoft.opensearch.test.OpenSearchTestCase

class RetryFailedManagedIndexRequestTests : OpenSearchTestCase() {

    fun `test retry managed index request`() {
        val indices = listOf("index1", "index2")
        val startState = "state1"
        val clusterManagerTimeout = TimeValue.timeValueSeconds(30)
        val req = RetryFailedManagedIndexRequest(indices, startState, clusterManagerTimeout, DEFAULT_INDEX_TYPE)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = RetryFailedManagedIndexRequest(sin)
        assertEquals(indices, newReq.indices)
        assertEquals(startState, newReq.startState)
    }

    fun `test retry managed index request with non default index type and multiple indices fails`() {
        val indices = listOf("index1", "index2")
        val startState = "state1"
        val clusterManagerTimeout = TimeValue.timeValueSeconds(30)
        val req = RetryFailedManagedIndexRequest(indices, startState, clusterManagerTimeout, "non-existent-index-type")

        val actualException: String? = req.validate()?.validationErrors()?.firstOrNull()
        val expectedException: String = RetryFailedManagedIndexRequest.MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR
        assertEquals("Retry failed managed index request should have failed validation with specific exception", actualException, expectedException)
    }
}
