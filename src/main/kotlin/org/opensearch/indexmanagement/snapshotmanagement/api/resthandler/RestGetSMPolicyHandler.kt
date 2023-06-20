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

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_TYPE
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICIES_ACTION_TYPE
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPoliciesRequest
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyRequest
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.util.DEFAULT_SM_POLICY_SORT_FIELD
import com.colasoft.opensearch.indexmanagement.util.getSearchParams
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.GET
import com.colasoft.opensearch.rest.action.RestToXContentListener

class RestGetSMPolicyHandler : BaseRestHandler() {

    override fun getName(): String {
        return "snapshot_management_get_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "$SM_POLICIES_URI/{policyName}"),
            Route(GET, "$SM_POLICIES_URI/")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        return if (policyName.isEmpty()) {
            getAllPolicies(request, client)
        } else {
            getSMPolicyByName(client, policyName)
        }
    }

    private fun getSMPolicyByName(client: NodeClient, policyName: String): RestChannelConsumer {
        return RestChannelConsumer {
            client.execute(GET_SM_POLICY_ACTION_TYPE, GetSMPolicyRequest(smPolicyNameToDocId(policyName)), RestToXContentListener(it))
        }
    }

    private fun getAllPolicies(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val searchParams = request.getSearchParams(DEFAULT_SM_POLICY_SORT_FIELD)

        return RestChannelConsumer {
            client.execute(GET_SM_POLICIES_ACTION_TYPE, GetSMPoliciesRequest(searchParams), RestToXContentListener(it))
        }
    }
}
