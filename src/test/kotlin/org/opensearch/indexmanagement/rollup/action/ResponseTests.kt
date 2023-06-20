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

package com.colasoft.opensearch.indexmanagement.rollup.action

import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.indexmanagement.rollup.action.explain.ExplainRollupResponse
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupResponse
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsResponse
import com.colasoft.opensearch.indexmanagement.rollup.action.index.IndexRollupResponse
import com.colasoft.opensearch.indexmanagement.rollup.randomExplainRollup
import com.colasoft.opensearch.indexmanagement.rollup.randomRollup
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.test.OpenSearchTestCase

class ResponseTests : OpenSearchTestCase() {

    fun `test explain rollup response`() {
        val idsToExplain = randomList(10) { randomAlphaOfLength(10) to randomExplainRollup() }.toMap()
        val res = ExplainRollupResponse(idsToExplain)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = ExplainRollupResponse(sin)
        assertEquals(idsToExplain, streamedRes.idsToExplain)
    }

    fun `test get rollup response null`() {
        val res = GetRollupResponse("someid", 1L, 2L, 3L, RestStatus.OK, null)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = GetRollupResponse(sin)
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(null, streamedRes.rollup)
    }

    fun `test get rollup response`() {
        val rollup = randomRollup()
        val res = GetRollupResponse("someid", 1L, 2L, 3L, RestStatus.OK, rollup)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = GetRollupResponse(sin)
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(rollup, streamedRes.rollup)
    }

    fun `test get rollups response`() {
        val rollups = randomList(1, 15) { randomRollup() }
        val res = GetRollupsResponse(rollups, rollups.size, RestStatus.OK)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = GetRollupsResponse(sin)
        assertEquals(rollups.size, streamedRes.totalRollups)
        assertEquals(rollups.size, streamedRes.rollups.size)
        assertEquals(RestStatus.OK, streamedRes.status)
        for (i in 0 until rollups.size) {
            assertEquals(rollups[i], streamedRes.rollups[i])
        }
    }

    fun `test index rollup response`() {
        val rollup = randomRollup()
        val res = IndexRollupResponse("someid", 1L, 2L, 3L, RestStatus.OK, rollup)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = IndexRollupResponse(sin)
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(rollup, streamedRes.rollup)
    }
}
