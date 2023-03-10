/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_POLICY_SORT_FIELD
import com.colasoft.opensearch.indexmanagement.util.getSearchParams
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.GET
import com.colasoft.opensearch.rest.RestRequest.Method.HEAD
import com.colasoft.opensearch.rest.action.RestActions
import com.colasoft.opensearch.rest.action.RestToXContentListener
import com.colasoft.opensearch.search.fetch.subphase.FetchSourceContext

private val log = LogManager.getLogger(RestGetPolicyAction::class.java)

class RestGetPolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, POLICY_BASE_URI,
                GET, LEGACY_POLICY_BASE_URI
            ),
            ReplacedRoute(
                GET, "$POLICY_BASE_URI/{policyID}",
                GET, "$LEGACY_POLICY_BASE_URI/{policyID}"
            ),
            ReplacedRoute(
                HEAD, "$POLICY_BASE_URI/{policyID}",
                HEAD, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String {
        return "get_policy_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val policyId = request.param("policyID")

        var fetchSrcContext: FetchSourceContext = FetchSourceContext.FETCH_SOURCE
        if (request.method() == HEAD) {
            fetchSrcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val searchParams = request.getSearchParams(DEFAULT_POLICY_SORT_FIELD)

        return RestChannelConsumer { channel ->
            if (policyId == null || policyId.isEmpty()) {
                val getPoliciesRequest = GetPoliciesRequest(searchParams)
                client.execute(GetPoliciesAction.INSTANCE, getPoliciesRequest, RestToXContentListener(channel))
            } else {
                val getPolicyRequest = GetPolicyRequest(policyId, RestActions.parseVersion(request), fetchSrcContext)
                client.execute(GetPolicyAction.INSTANCE, getPolicyRequest, RestToXContentListener(channel))
            }
        }
    }
}
