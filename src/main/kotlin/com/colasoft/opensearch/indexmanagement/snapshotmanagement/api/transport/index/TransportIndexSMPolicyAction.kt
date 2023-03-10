/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.action.index.IndexResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.util.concurrent.ThreadContext
import com.colasoft.opensearch.common.xcontent.ToXContent
import com.colasoft.opensearch.common.xcontent.XContentFactory
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.indexmanagement.IndexManagementIndices
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.opensearchapi.suspendUntil
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.INDEX_SM_POLICY_ACTION_NAME
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import com.colasoft.opensearch.indexmanagement.util.IndexUtils
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils
import com.colasoft.opensearch.transport.TransportService

class TransportIndexSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    private val indexManagementIndices: IndexManagementIndices,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<IndexSMPolicyRequest, IndexSMPolicyResponse>(
    INDEX_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::IndexSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: IndexSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): IndexSMPolicyResponse {
        // If filterBy is enabled and security is disabled or if filter by is enabled and backend role are empty an exception will be thrown
        SecurityUtils.validateUserConfiguration(user, filterByEnabled)

        if (indexManagementIndices.checkAndUpdateIMConfigIndex(log)) {
            log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
        }
        return indexSMPolicy(request, user)
    }

    private suspend fun indexSMPolicy(request: IndexSMPolicyRequest, user: User?): IndexSMPolicyResponse {
        val policy = request.policy.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = user)
        val indexReq = request.index(INDEX_MANAGEMENT_INDEX)
            .source(policy.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .id(policy.id)
            .routing(policy.id) // by default routed by id
        val indexRes: IndexResponse = client.suspendUntil { index(indexReq, it) }

        return IndexSMPolicyResponse(indexRes.id, indexRes.version, indexRes.seqNo, indexRes.primaryTerm, policy, indexRes.status())
    }
}
