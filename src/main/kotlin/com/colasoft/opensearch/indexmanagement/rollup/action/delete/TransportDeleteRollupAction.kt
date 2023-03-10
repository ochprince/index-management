/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.delete

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
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.rollup.util.parseRollup
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService
import java.lang.Exception

@Suppress("ReturnCount")
class TransportDeleteRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val clusterService: ClusterService,
    val settings: Settings,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeleteRollupRequest, DeleteResponse>(
    DeleteRollupAction.NAME, transportService, actionFilters, ::DeleteRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: DeleteRollupRequest, actionListener: ActionListener<DeleteResponse>) {
        DeleteRollupHandler(client, actionListener, request).start()
    }

    inner class DeleteRollupHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteResponse>,
        private val request: DeleteRollupRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext)
    ) {

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            client.threadPool().threadContext.stashContext().use {
                getRollup()
            }
        }

        private fun getRollup() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id())
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(OpenSearchStatusException("Rollup ${request.id()} is not found", RestStatus.NOT_FOUND))
                            return
                        }

                        val rollup: Rollup?
                        try {
                            rollup = parseRollup(response, xContentRegistry)
                        } catch (e: IllegalArgumentException) {
                            actionListener.onFailure(OpenSearchStatusException("Rollup ${request.id()} is not found", RestStatus.NOT_FOUND))
                            return
                        }
                        if (!userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", rollup.id, actionListener)) {
                            return
                        } else {
                            delete()
                        }
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    }
                }
            )
        }

        private fun delete() {
            val deleteRequest = DeleteRequest(INDEX_MANAGEMENT_INDEX, request.id())
                .setRefreshPolicy(request.refreshPolicy)
            client.threadPool().threadContext.stashContext().use {
                client.delete(deleteRequest, actionListener)
            }
        }
    }
}
