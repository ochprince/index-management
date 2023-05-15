/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.colasoft.opensearch.action.support.WriteRequest.RefreshPolicy
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyRequest
import com.colasoft.opensearch.indexmanagement.util.REFRESH
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.DELETE
import com.colasoft.opensearch.rest.action.RestStatusToXContentListener
import java.io.IOException

class RestDeletePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                DELETE, "$POLICY_BASE_URI/{policyID}",
                DELETE, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String = "delete_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyId = request.param("policyID")
        if (policyId == null || policyId.isEmpty()) {
            throw IllegalArgumentException("Missing policy ID")
        }

        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        val deletePolicyRequest = DeletePolicyRequest(policyId, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeletePolicyAction.INSTANCE, deletePolicyRequest, RestStatusToXContentListener(channel))
        }
    }
}
