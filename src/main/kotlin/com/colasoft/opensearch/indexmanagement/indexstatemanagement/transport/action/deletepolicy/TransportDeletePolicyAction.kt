/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.delete.DeleteRequest
import com.colasoft.opensearch.action.delete.DeleteResponse
import com.colasoft.opensearch.action.get.GetRequest
import com.colasoft.opensearch.action.get.GetResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService
import java.lang.IllegalArgumentException

private val log = LogManager.getLogger(TransportDeletePolicyAction::class.java)

@Suppress("ReturnCount")
class TransportDeletePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeletePolicyRequest, DeleteResponse>(
    DeletePolicyAction.NAME, transportService, actionFilters, ::DeletePolicyRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: DeletePolicyRequest, listener: ActionListener<DeleteResponse>) {
        DeletePolicyHandler(client, listener, request).start()
    }

    inner class DeletePolicyHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteResponse>,
        private val request: DeletePolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            client.threadPool().threadContext.stashContext().use {
                getPolicy()
            }
        }

        private fun getPolicy() {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(OpenSearchStatusException("Policy ${request.policyID} is not found", RestStatus.NOT_FOUND))
                            return
                        }

                        val policy: Policy?
                        try {
                            policy = parseFromGetResponse(response, xContentRegistry, Policy.Companion::parse)
                        } catch (e: IllegalArgumentException) {
                            actionListener.onFailure(OpenSearchStatusException("Policy ${request.policyID} is not found", RestStatus.NOT_FOUND))
                            return
                        }
                        if (!userHasPermissionForResource(user, policy.user, filterByEnabled, "policy", request.policyID, actionListener)) {
                            return
                        } else {
                            delete()
                        }
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        private fun delete() {
            val deleteRequest = DeleteRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .setRefreshPolicy(request.refreshPolicy)

            client.threadPool().threadContext.stashContext().use {
                client.delete(deleteRequest, actionListener)
            }
        }
    }
}
