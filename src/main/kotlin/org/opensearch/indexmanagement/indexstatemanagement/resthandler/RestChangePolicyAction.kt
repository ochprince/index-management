/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.core.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyRequest
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

class RestChangePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, CHANGE_POLICY_BASE_URI,
                POST, LEGACY_CHANGE_POLICY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$CHANGE_POLICY_BASE_URI/{index}",
                POST, "$LEGACY_CHANGE_POLICY_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String = "change_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing index")
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val xcp = request.contentParser()
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
        val changePolicy = ChangePolicy.parse(xcp)

        val changePolicyRequest = ChangePolicyRequest(indices.toList(), changePolicy, indexType)

        return RestChannelConsumer { channel ->
            client.execute(ChangePolicyAction.INSTANCE, changePolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val CHANGE_POLICY_BASE_URI = "$ISM_BASE_URI/change_policy"
        const val LEGACY_CHANGE_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/change_policy"
        const val INDEX_NOT_MANAGED = "This index is not being managed"
        const val INDEX_IN_TRANSITION = "Cannot change policy while transitioning to new state"
    }
}
