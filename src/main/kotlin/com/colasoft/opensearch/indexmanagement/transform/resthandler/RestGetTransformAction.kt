/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformRequest
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsAction
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsRequest
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_FROM
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SEARCH_STRING
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SIZE
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SORT_DIRECTION
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SORT_FIELD
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.GET
import com.colasoft.opensearch.rest.RestRequest.Method.HEAD
import com.colasoft.opensearch.rest.action.RestToXContentListener
import com.colasoft.opensearch.search.fetch.subphase.FetchSourceContext

class RestGetTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, TRANSFORM_BASE_URI),
            Route(GET, "$TRANSFORM_BASE_URI/{transformID}"),
            Route(HEAD, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String {
        return "opendistro_get_transform_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformID = request.param("transformID")
        val searchString = request.param("search", DEFAULT_SEARCH_STRING)
        val from = request.paramAsInt("from", DEFAULT_FROM)
        val size = request.paramAsInt("size", DEFAULT_SIZE)
        val sortField = request.param("sortField", DEFAULT_SORT_FIELD)
        val sortDirection = request.param("sortDirection", DEFAULT_SORT_DIRECTION)
        return RestChannelConsumer { channel ->
            if (transformID == null || transformID.isEmpty()) {
                val req = GetTransformsRequest(
                    searchString,
                    from,
                    size,
                    sortField,
                    sortDirection
                )
                client.execute(GetTransformsAction.INSTANCE, req, RestToXContentListener(channel))
            } else {
                val req = GetTransformRequest(transformID, if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null)
                client.execute(GetTransformAction.INSTANCE, req, RestToXContentListener(channel))
            }
        }
    }
}
