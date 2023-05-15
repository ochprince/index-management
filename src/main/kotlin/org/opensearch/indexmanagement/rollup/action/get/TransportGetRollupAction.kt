/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.get

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.get.GetRequest
import com.colasoft.opensearch.action.get.GetResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.rollup.util.parseRollup
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService
import java.lang.Exception

class TransportGetRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val settings: Settings,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetRollupRequest, GetRollupResponse> (
    GetRollupAction.NAME, transportService, actionFilters, ::GetRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    @Suppress("ReturnCount")
    override fun doExecute(task: Task, request: GetRollupRequest, listener: ActionListener<GetRollupResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id).preference(request.preference)
        val user = buildUser(client.threadPool().threadContext)
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            return listener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                        }

                        val rollup: Rollup?
                        try {
                            rollup = parseRollup(response, xContentRegistry)
                        } catch (e: IllegalArgumentException) {
                            listener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                            return
                        }
                        if (!SecurityUtils.userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", request.id, listener)) {
                            return
                        } else {
                            // if HEAD request don't return the rollup
                            val rollupResponse = if (request.srcContext != null && !request.srcContext.fetchSource()) {
                                GetRollupResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, null)
                            } else {
                                GetRollupResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, rollup)
                            }
                            listener.onResponse(rollupResponse)
                        }
                    }

                    override fun onFailure(e: Exception) {
                        listener.onFailure(e)
                    }
                }
            )
        }
    }
}
