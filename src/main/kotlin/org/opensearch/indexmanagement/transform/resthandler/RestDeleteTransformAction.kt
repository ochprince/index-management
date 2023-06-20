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
import com.colasoft.opensearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import com.colasoft.opensearch.indexmanagement.transform.action.delete.DeleteTransformsRequest
import com.colasoft.opensearch.indexmanagement.transform.action.delete.DeleteTransformsRequest.Companion.DEFAULT_FORCE_DELETE
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.DELETE
import com.colasoft.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestDeleteTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            Route(DELETE, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String = "opendistro_delete_transform_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformID = request.param("transformID")
        val force = request.paramAsBoolean("force", DEFAULT_FORCE_DELETE)
        return RestChannelConsumer { channel ->
            channel.newBuilder()
            val deleteTransformsRequest = DeleteTransformsRequest(transformID.split(","), force)
            client.execute(DeleteTransformsAction.INSTANCE, deleteTransformsRequest, RestToXContentListener(channel))
        }
    }
}
