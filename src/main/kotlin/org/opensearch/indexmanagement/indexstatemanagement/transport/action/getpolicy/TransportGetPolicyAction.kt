/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.get.GetRequest
import com.colasoft.opensearch.action.get.GetResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService
import java.lang.IllegalArgumentException

@Suppress("ReturnCount")
class TransportGetPolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetPolicyRequest, GetPolicyResponse>(
    GetPolicyAction.NAME, transportService, actionFilters, ::GetPolicyRequest
) {

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: GetPolicyRequest, listener: ActionListener<GetPolicyResponse>) {
        GetPolicyHandler(client, listener, request).start()
    }

    inner class GetPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<GetPolicyResponse>,
        private val request: GetPolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .version(request.version)

            client.threadPool().threadContext.stashContext().use {
                client.get(
                    getRequest,
                    object : ActionListener<GetResponse> {
                        override fun onResponse(response: GetResponse) {
                            onGetResponse(response)
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                )
            }
        }

        fun onGetResponse(response: GetResponse) {
            if (!response.isExists) {
                actionListener.onFailure(OpenSearchStatusException("Policy not found", RestStatus.NOT_FOUND))
                return
            }

            val policy: Policy?
            try {
                policy = parseFromGetResponse(response, xContentRegistry, Policy.Companion::parse)
            } catch (e: IllegalArgumentException) {
                actionListener.onFailure(OpenSearchStatusException("Policy not found", RestStatus.NOT_FOUND))
                return
            }
            if (!userHasPermissionForResource(user, policy.user, filterByEnabled, "policy", request.policyID, actionListener)) {
                return
            } else {
                // if HEAD request don't return the policy
                val policyResponse = if (!request.fetchSrcContext.fetchSource()) {
                    GetPolicyResponse(response.id, response.version, response.seqNo, response.primaryTerm, null)
                } else {
                    GetPolicyResponse(response.id, response.version, response.seqNo, response.primaryTerm, policy)
                }
                actionListener.onResponse(policyResponse)
            }
        }
    }
}
