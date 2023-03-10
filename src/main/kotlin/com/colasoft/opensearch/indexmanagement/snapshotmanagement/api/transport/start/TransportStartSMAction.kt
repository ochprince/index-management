/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.start

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.DocWriteResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.action.update.UpdateResponse
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
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.getSMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.verifyUserHasPermissionForResource
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.transport.TransportService
import java.time.Instant

class TransportStartSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<StartSMRequest, AcknowledgedResponse>(
    SMActions.START_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::StartSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: StartSMRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): AcknowledgedResponse {
        val smPolicy = client.getSMPolicy(request.id())

        // Check if the requested user has permission on the resource, throwing an exception if the user does not
        verifyUserHasPermissionForResource(user, smPolicy.user, filterByEnabled, "snapshot management policy", smPolicy.policyName)

        if (smPolicy.jobEnabled) {
            log.debug("Snapshot management policy is already enabled")
            return AcknowledgedResponse(true)
        }
        return AcknowledgedResponse(enableSMPolicy(request))
    }

    private suspend fun enableSMPolicy(updateRequest: StartSMRequest): Boolean {
        val now = Instant.now().toEpochMilli()
        updateRequest.index(INDEX_MANAGEMENT_INDEX).doc(
            mapOf(
                SMPolicy.SM_TYPE to mapOf(
                    SMPolicy.ENABLED_FIELD to true,
                    SMPolicy.ENABLED_TIME_FIELD to now,
                    SMPolicy.LAST_UPDATED_TIME_FIELD to now
                )
            )
        )
        val updateResponse: UpdateResponse = try {
            client.suspendUntil { update(updateRequest, it) }
        } catch (e: VersionConflictEngineException) {
            log.error("VersionConflictEngineException while trying to enable snapshot management policy id [${updateRequest.id()}]: $e")
            throw OpenSearchStatusException(conflictExceptionMessage, RestStatus.INTERNAL_SERVER_ERROR)
        } catch (e: Exception) {
            log.error("Failed trying to enable snapshot management policy id [${updateRequest.id()}]: $e")
            throw OpenSearchStatusException("Failed while trying to enable SM Policy", RestStatus.INTERNAL_SERVER_ERROR)
        }
        return updateResponse.result == DocWriteResponse.Result.UPDATED
    }

    companion object {
        private const val conflictExceptionMessage = "Failed while trying to enable SM Policy due to a concurrent update, please try again"
    }
}
