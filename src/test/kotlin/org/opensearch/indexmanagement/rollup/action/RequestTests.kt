/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action

import com.colasoft.opensearch.action.DocWriteRequest
import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.index.seqno.SequenceNumbers
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.rollup.action.delete.DeleteRollupRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.index.IndexRollupRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.start.StartRollupRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.stop.StopRollupRequest
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.rollup.randomRollup
import com.colasoft.opensearch.search.fetch.subphase.FetchSourceContext
import com.colasoft.opensearch.test.OpenSearchTestCase

class RequestTests : OpenSearchTestCase() {

    fun `test delete rollup request`() {
        val id = "some_id"
        val req = DeleteRollupRequest(id).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = DeleteRollupRequest(sin)
        assertEquals(id, streamedReq.id())
    }

    fun `test explain rollup request`() {
        val ids = listOf("oneid", "twoid", "threeid")
        val req = ExplainRollupRequest(ids)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = ExplainRollupRequest(sin)
        assertEquals(ids, streamedReq.rollupIDs)
    }

    fun `test get rollup request`() {
        val id = "some_id"
        val srcContext = null
        val preference = "_local"
        val req = GetRollupRequest(id, srcContext, preference)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = GetRollupRequest(sin)
        assertEquals(id, streamedReq.id)
        assertEquals(srcContext, streamedReq.srcContext)
        assertEquals(preference, streamedReq.preference)
    }

    fun `test head get rollup request`() {
        val id = "some_id"
        val srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        val req = GetRollupRequest(id, srcContext)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = GetRollupRequest(sin)
        assertEquals(id, streamedReq.id)
        assertEquals(srcContext, streamedReq.srcContext)
    }

    fun `test get rollups request default`() {
        val req = GetRollupsRequest()

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = GetRollupsRequest(sin)
        assertEquals("", streamedReq.searchString)
        assertEquals(0, streamedReq.from)
        assertEquals(20, streamedReq.size)
        assertEquals("${Rollup.ROLLUP_TYPE}.${Rollup.ROLLUP_ID_FIELD}.keyword", streamedReq.sortField)
        assertEquals("asc", streamedReq.sortDirection)
    }

    fun `test get rollups request`() {
        val req = GetRollupsRequest("searching", 10, 50, "sorted", "desc")

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = GetRollupsRequest(sin)
        assertEquals("searching", streamedReq.searchString)
        assertEquals(10, streamedReq.from)
        assertEquals(50, streamedReq.size)
        assertEquals("sorted", streamedReq.sortField)
        assertEquals("desc", streamedReq.sortDirection)
    }

    fun `test index rollup post request`() {
        val rollup = randomRollup().copy(seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val req = IndexRollupRequest(
            rollup = rollup,
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        ).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = IndexRollupRequest(sin)
        assertEquals(rollup, streamedReq.rollup)
        assertEquals(rollup.seqNo, streamedReq.ifSeqNo())
        assertEquals(rollup.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
        assertEquals(DocWriteRequest.OpType.CREATE, streamedReq.opType())
    }

    fun `test index rollup put request`() {
        val rollup = randomRollup().copy(seqNo = 1L, primaryTerm = 2L)
        val req = IndexRollupRequest(
            rollup = rollup,
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        ).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = IndexRollupRequest(sin)
        assertEquals(rollup, streamedReq.rollup)
        assertEquals(rollup.seqNo, streamedReq.ifSeqNo())
        assertEquals(rollup.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
        assertEquals(DocWriteRequest.OpType.INDEX, streamedReq.opType())
    }

    fun `test start rollup request`() {
        val id = "some_id"
        val req = StartRollupRequest(id).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = StartRollupRequest(sin)
        assertEquals(id, streamedReq.id())
    }

    fun `test stop rollup request`() {
        val id = "some_id"
        val req = StopRollupRequest(id).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = StopRollupRequest(sin)
        assertEquals(id, streamedReq.id())
    }
}
