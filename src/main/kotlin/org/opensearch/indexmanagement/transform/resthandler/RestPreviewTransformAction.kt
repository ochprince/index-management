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
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.transform.action.preview.PreviewTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.preview.PreviewTransformRequest
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.POST
import com.colasoft.opensearch.rest.action.RestToXContentListener

class RestPreviewTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(POST, TRANSFORM_BASE_URI),
            RestHandler.Route(POST, "$TRANSFORM_BASE_URI/_preview")
        )
    }

    override fun getName(): String {
        return "opendistro_preview_transform_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val xcp = request.contentParser()
        val transform = xcp.parseWithType(parse = Transform.Companion::parse)
        val previewTransformRequest = PreviewTransformRequest(transform)
        return RestChannelConsumer { channel ->
            client.execute(PreviewTransformAction.INSTANCE, previewTransformRequest, RestToXContentListener(channel))
        }
    }
}
