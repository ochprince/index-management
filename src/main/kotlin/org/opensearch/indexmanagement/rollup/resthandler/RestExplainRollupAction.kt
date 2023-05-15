/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.GET
import com.colasoft.opensearch.rest.action.RestToXContentListener

class RestExplainRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_explain",
                GET, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}/_explain"
            )
        )
    }

    override fun getName(): String = "opendistro_explain_rollup_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupIDs: List<String> = Strings.splitStringByCommaToArray(request.param("rollupID")).toList()
        if (rollupIDs.isEmpty()) {
            throw IllegalArgumentException("Missing rollupID")
        }
        val explainRequest = ExplainRollupRequest(rollupIDs)
        return RestChannelConsumer { channel ->
            client.execute(ExplainRollupAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
