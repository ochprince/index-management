/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.resthandler

import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.common.xcontent.ToXContent
import com.colasoft.opensearch.index.seqno.SequenceNumbers
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.transform.action.index.IndexTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.index.IndexTransformRequest
import com.colasoft.opensearch.indexmanagement.transform.action.index.IndexTransformResponse
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import com.colasoft.opensearch.indexmanagement.util.IF_SEQ_NO
import com.colasoft.opensearch.indexmanagement.util.NO_ID
import com.colasoft.opensearch.indexmanagement.util.REFRESH
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.BytesRestResponse
import com.colasoft.opensearch.rest.RestChannel
import com.colasoft.opensearch.rest.RestHandler
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.PUT
import com.colasoft.opensearch.rest.RestResponse
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(PUT, TRANSFORM_BASE_URI),
            RestHandler.Route(PUT, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String {
        return "opendistro_index_transform_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("transformID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing transform ID")
        }

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val transform = xcp.parseWithType(id = id, seqNo = seqNo, primaryTerm = primaryTerm, parse = Transform.Companion::parse)
            .copy(updatedAt = Instant.now())
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexTransformRequest = IndexTransformRequest(transform, refreshPolicy)
        return RestChannelConsumer { channel ->
            client.execute(IndexTransformAction.INSTANCE, indexTransformRequest, indexTransformResponse(channel))
        }
    }

    private fun indexTransformResponse(channel: RestChannel):
        RestResponseListener<IndexTransformResponse> {
        return object : RestResponseListener<IndexTransformResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexTransformResponse): RestResponse {
                val restResponse =
                    BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (response.status == RestStatus.CREATED) {
                    val location = "$TRANSFORM_BASE_URI/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
