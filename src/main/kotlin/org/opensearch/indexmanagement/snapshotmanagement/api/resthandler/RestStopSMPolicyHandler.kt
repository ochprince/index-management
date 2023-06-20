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

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.stop.StopSMRequest
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.util.getValidSMPolicyName
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.action.RestToXContentListener

class RestStopSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(RestStopSMPolicyHandler::class.java)

    override fun getName(): String {
        return "snapshot_management_stop_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(RestRequest.Method.POST, "$SM_POLICIES_URI/{policyName}/_stop")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.getValidSMPolicyName()
        log.debug("Stop snapshot management policy request received with policy name [$policyName]")

        val indexReq = StopSMRequest(smPolicyNameToDocId(policyName))
        return RestChannelConsumer {
            client.execute(
                SMActions.STOP_SM_POLICY_ACTION_TYPE,
                indexReq, RestToXContentListener(it)
            )
        }
    }
}
