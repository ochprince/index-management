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

package com.colasoft.opensearch.indexmanagement.rollup.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.rollup.action.stop.StopRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.stop.StopRollupRequest
import com.colasoft.opensearch.indexmanagement.util.NO_ID
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.POST
import com.colasoft.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestStopRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_stop",
                POST, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}/_stop"
            )
        )
    }

    override fun getName(): String {
        return "opendistro_stop_rollup_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("rollupID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing rollup ID")
        }

        val stopRequest = StopRollupRequest(id)
        return RestChannelConsumer { channel ->
            client.execute(StopRollupAction.INSTANCE, stopRequest, RestToXContentListener(channel))
        }
    }
}
