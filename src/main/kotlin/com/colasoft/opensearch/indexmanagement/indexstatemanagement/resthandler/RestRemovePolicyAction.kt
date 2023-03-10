/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.POST
import com.colasoft.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestRemovePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, REMOVE_POLICY_BASE_URI,
                POST, LEGACY_REMOVE_POLICY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$REMOVE_POLICY_BASE_URI/{index}",
                POST, "$LEGACY_REMOVE_POLICY_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String = "remove_policy_action"

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String> = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val removePolicyRequest = RemovePolicyRequest(indices.toList(), indexType)

        return RestChannelConsumer { channel ->
            client.execute(RemovePolicyAction.INSTANCE, removePolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val REMOVE_POLICY_BASE_URI = "$ISM_BASE_URI/remove"
        const val LEGACY_REMOVE_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/remove"
    }
}
