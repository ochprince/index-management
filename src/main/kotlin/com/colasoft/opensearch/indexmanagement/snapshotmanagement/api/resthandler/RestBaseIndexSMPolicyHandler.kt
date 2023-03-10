/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.common.xcontent.ToXContent
import com.colasoft.opensearch.index.seqno.SequenceNumbers
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyRequest
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyResponse
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.util.getValidSMPolicyName
import com.colasoft.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import com.colasoft.opensearch.indexmanagement.util.IF_SEQ_NO
import com.colasoft.opensearch.indexmanagement.util.REFRESH
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BytesRestResponse
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestResponse
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.rest.action.RestResponseListener
import java.time.Instant

abstract class RestBaseIndexSMPolicyHandler : BaseRestHandler() {

    protected fun prepareIndexRequest(request: RestRequest, client: NodeClient, create: Boolean): RestChannelConsumer {
        val policyName = request.getValidSMPolicyName()

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val policy = SMPolicy.parse(xcp, id = smPolicyNameToDocId(policyName), seqNo = seqNo, primaryTerm = primaryTerm)
            .copy(jobLastUpdateTime = Instant.now())

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        return RestChannelConsumer {
            client.execute(
                SMActions.INDEX_SM_POLICY_ACTION_TYPE,
                IndexSMPolicyRequest(policy, create, refreshPolicy),
                object : RestResponseListener<IndexSMPolicyResponse>(it) {
                    override fun buildResponse(response: IndexSMPolicyResponse): RestResponse {
                        val restResponse = BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                        if (response.status == RestStatus.CREATED || response.status == RestStatus.OK) {
                            val location = "$SM_POLICIES_URI/${response.policy.policyName}"
                            restResponse.addHeader("Location", location)
                        }
                        return restResponse
                    }
                }
            )
        }
    }
}
