/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.get.GetRequest
import com.colasoft.opensearch.action.get.GetResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.util.concurrent.ThreadContext
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.index.IndexNotFoundException
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.opensearchapi.suspendUntil
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_NAME
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.parseSMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.verifyUserHasPermissionForResource
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.transport.TransportService

class TransportGetSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<GetSMPolicyRequest, GetSMPolicyResponse>(
    GET_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::GetSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: GetSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): GetSMPolicyResponse {
        val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
        val getResponse: GetResponse = try {
            client.suspendUntil { get(getRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }
        if (!getResponse.isExists) {
            throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.NOT_FOUND)
        }
        val smPolicy = try {
            parseSMPolicy(getResponse)
        } catch (e: IllegalArgumentException) {
            log.error("Error while parsing snapshot management policy ${request.policyID}", e)
            throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.INTERNAL_SERVER_ERROR)
        }

        // Check if the requested user has permission on the resource, throwing an exception if the user does not
        verifyUserHasPermissionForResource(user, smPolicy.user, filterByEnabled, "snapshot management policy", smPolicy.policyName)

        log.debug("Get SM policy: $smPolicy")
        return GetSMPolicyResponse(getResponse.id, getResponse.version, getResponse.seqNo, getResponse.primaryTerm, smPolicy)
    }
}
