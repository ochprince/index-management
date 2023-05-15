/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.resthandler

import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.index.seqno.SequenceNumbers
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.index.IndexRollupRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.index.IndexRollupResponse
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import com.colasoft.opensearch.indexmanagement.util.IF_SEQ_NO
import com.colasoft.opensearch.indexmanagement.util.NO_ID
import com.colasoft.opensearch.indexmanagement.util.REFRESH
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.BytesRestResponse
import com.colasoft.opensearch.rest.RestChannel
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.PUT
import com.colasoft.opensearch.rest.RestResponse
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                PUT, ROLLUP_JOBS_BASE_URI,
                PUT, LEGACY_ROLLUP_JOBS_BASE_URI
            ),
            ReplacedRoute(
                PUT, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                PUT, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            )
        )
    }

    override fun getName(): String {
        return "opendistro_index_rollup_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("rollupID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing rollup ID")
        }

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val rollup = xcp.parseWithType(id = id, seqNo = seqNo, primaryTerm = primaryTerm, parse = Rollup.Companion::parse)
            .copy(jobLastUpdatedTime = Instant.now())
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexRollupRequest = IndexRollupRequest(rollup, refreshPolicy)
        return RestChannelConsumer { channel ->
            client.execute(IndexRollupAction.INSTANCE, indexRollupRequest, indexRollupResponse(channel))
        }
    }

    private fun indexRollupResponse(channel: RestChannel):
        RestResponseListener<IndexRollupResponse> {
        return object : RestResponseListener<IndexRollupResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexRollupResponse): RestResponse {
                val restResponse =
                    BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (response.status == RestStatus.CREATED) {
                    val location = "$ROLLUP_JOBS_BASE_URI/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
