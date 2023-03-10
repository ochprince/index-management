/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.xcontent.ToXContent
import com.colasoft.opensearch.index.seqno.SequenceNumbers
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyResponse
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import com.colasoft.opensearch.indexmanagement.util.IF_SEQ_NO
import com.colasoft.opensearch.indexmanagement.util.NO_ID
import com.colasoft.opensearch.indexmanagement.util.REFRESH
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.BytesRestResponse
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.PUT
import com.colasoft.opensearch.rest.RestResponse
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexPolicyAction(
    settings: Settings,
    val clusterService: ClusterService
) : BaseRestHandler() {

    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                PUT, POLICY_BASE_URI,
                PUT, LEGACY_POLICY_BASE_URI
            ),
            ReplacedRoute(
                PUT, "$POLICY_BASE_URI/{policyID}",
                PUT, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String {
        return "index_policy_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("policyID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing policy ID")
        }

        val xcp = request.contentParser()
        val policy = xcp.parseWithType(id = id, parse = Policy.Companion::parse).copy(lastUpdatedTime = Instant.now())
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val disallowedActions = policy.getDisallowedActions(allowList)
        if (disallowedActions.isNotEmpty()) {
            return RestChannelConsumer { channel ->
                channel.sendResponse(
                    BytesRestResponse(
                        RestStatus.FORBIDDEN,
                        "You have actions that are not allowed in your policy $disallowedActions"
                    )
                )
            }
        }

        val indexPolicyRequest = IndexPolicyRequest(id, policy, seqNo, primaryTerm, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(
                IndexPolicyAction.INSTANCE, indexPolicyRequest,
                object : RestResponseListener<IndexPolicyResponse>(channel) {
                    override fun buildResponse(response: IndexPolicyResponse): RestResponse {
                        val restResponse = BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                        if (response.status == RestStatus.CREATED) {
                            val location = "$POLICY_BASE_URI/${response.id}"
                            restResponse.addHeader("Location", location)
                        }
                        return restResponse
                    }
                }
            )
        }
    }
}
