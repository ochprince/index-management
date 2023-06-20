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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.common.xcontent.XContentHelper
import com.colasoft.opensearch.core.xcontent.MediaType
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.POST
import com.colasoft.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestAddPolicyAction : BaseRestHandler() {

    override fun getName(): String = "add_policy_action"

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, ADD_POLICY_BASE_URI,
                POST, LEGACY_ADD_POLICY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$ADD_POLICY_BASE_URI/{index}",
                POST, "$LEGACY_ADD_POLICY_BASE_URI/{index}"
            )
        )
    }

    @Throws(IOException::class)
    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType as (MediaType)).v2()
        } else {
            mapOf()
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val policyID = requireNotNull(body.getOrDefault("policy_id", null)) { "Missing policy_id" }

        val addPolicyRequest = AddPolicyRequest(indices.toList(), policyID as String, indexType)

        return RestChannelConsumer { channel ->
            client.execute(AddPolicyAction.INSTANCE, addPolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val ADD_POLICY_BASE_URI = "$ISM_BASE_URI/add"
        const val LEGACY_ADD_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/add"
    }
}
