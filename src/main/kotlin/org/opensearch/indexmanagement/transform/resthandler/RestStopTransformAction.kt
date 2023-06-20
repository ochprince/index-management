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

package com.colasoft.opensearch.indexmanagement.transform.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.colasoft.opensearch.indexmanagement.transform.action.stop.StopTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.stop.StopTransformRequest
import com.colasoft.opensearch.indexmanagement.util.NO_ID
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.POST
import com.colasoft.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestStopTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, "$TRANSFORM_BASE_URI/{transformID}/_stop")
        )
    }

    override fun getName(): String {
        return "opendistro_stop_transform_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("transformID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing transform ID")
        }

        val stopRequest = StopTransformRequest(id)
        return RestChannelConsumer { channel ->
            client.execute(StopTransformAction.INSTANCE, stopRequest, RestToXContentListener(channel))
        }
    }
}
