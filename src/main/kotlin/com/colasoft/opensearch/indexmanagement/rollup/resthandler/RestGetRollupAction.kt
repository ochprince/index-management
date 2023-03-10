/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsAction
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_FROM
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SEARCH_STRING
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SIZE
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SORT_DIRECTION
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SORT_FIELD
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.GET
import com.colasoft.opensearch.rest.RestRequest.Method.HEAD
import com.colasoft.opensearch.rest.action.RestToXContentListener
import com.colasoft.opensearch.search.fetch.subphase.FetchSourceContext

class RestGetRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, ROLLUP_JOBS_BASE_URI,
                GET, LEGACY_ROLLUP_JOBS_BASE_URI
            ),
            ReplacedRoute(
                GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                GET, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            ),
            ReplacedRoute(
                HEAD, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                HEAD, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            )
        )
    }

    override fun getName(): String {
        return "opendistro_get_rollup_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupID = request.param("rollupID")
        val searchString = request.param("search", DEFAULT_SEARCH_STRING)
        val from = request.paramAsInt("from", DEFAULT_FROM)
        val size = request.paramAsInt("size", DEFAULT_SIZE)
        val sortField = request.param("sortField", DEFAULT_SORT_FIELD)
        val sortDirection = request.param("sortDirection", DEFAULT_SORT_DIRECTION)
        return RestChannelConsumer { channel ->
            if (rollupID == null || rollupID.isEmpty()) {
                val req = GetRollupsRequest(
                    searchString,
                    from,
                    size,
                    sortField,
                    sortDirection
                )
                client.execute(GetRollupsAction.INSTANCE, req, RestToXContentListener(channel))
            } else {
                val req = GetRollupRequest(rollupID, if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null)
                client.execute(GetRollupAction.INSTANCE, req, RestToXContentListener(channel))
            }
        }
    }
}
