/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.DeleteSMPolicyRequest
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import com.colasoft.opensearch.indexmanagement.util.REFRESH
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.action.RestToXContentListener

class RestDeleteSMPolicyHandler : BaseRestHandler() {

    override fun getName(): String {
        return "snapshot_management_delete_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(RestRequest.Method.DELETE, "$SM_POLICIES_URI/{policyName}")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        return RestChannelConsumer {
            client.execute(
                SMActions.DELETE_SM_POLICY_ACTION_TYPE,
                DeleteSMPolicyRequest(smPolicyNameToDocId(policyName)).setRefreshPolicy(refreshPolicy),
                RestToXContentListener(it)
            )
        }
    }
}
