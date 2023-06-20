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

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.delete

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.delete.DeleteResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.util.concurrent.ThreadContext
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.index.engine.VersionConflictEngineException
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.opensearchapi.suspendUntil
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.DELETE_SM_POLICY_ACTION_NAME
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.getSMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.verifyUserHasPermissionForResource
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.transport.TransportService

class TransportDeleteSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<DeleteSMPolicyRequest, DeleteResponse>(
    DELETE_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::DeleteSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: DeleteSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): DeleteResponse {
        val smPolicy = client.getSMPolicy(request.id())

        // Check if the requested user has permission on the resource, throwing an exception if the user does not
        verifyUserHasPermissionForResource(user, smPolicy.user, filterByEnabled, "snapshot management policy", smPolicy.policyName)

        val deleteReq = request.index(INDEX_MANAGEMENT_INDEX)
        try {
            return client.suspendUntil { delete(deleteReq, it) }
        } catch (e: VersionConflictEngineException) {
            log.error("VersionConflictEngineException while trying to delete snapshot management policy id [${deleteReq.id()}]: $e")
            throw OpenSearchStatusException(conflictExceptionMessage, RestStatus.INTERNAL_SERVER_ERROR)
        } catch (e: Exception) {
            log.error("Failed trying to delete snapshot management policy id [${deleteReq.id()}]: $e")
            throw OpenSearchStatusException("Failed while trying to delete SM Policy", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }

    companion object {
        private const val conflictExceptionMessage = "Failed while trying to delete SM Policy due to a concurrent update, please try again"
    }
}
