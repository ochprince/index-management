/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.get

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
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
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService

class TransportGetTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetTransformRequest, GetTransformResponse> (
    GetTransformAction.NAME, transportService, actionFilters, ::GetTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    @Suppress("ReturnCount")
    override fun doExecute(task: Task, request: GetTransformRequest, listener: ActionListener<GetTransformResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val user = buildUser(client.threadPool().threadContext)
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id).preference(request.preference)
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            listener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                            return
                        }

                        try {
                            val transform: Transform?
                            try {
                                transform = parseFromGetResponse(response, xContentRegistry, Transform.Companion::parse)
                            } catch (e: IllegalArgumentException) {
                                listener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                                return
                            }
                            if (!userHasPermissionForResource(user, transform.user, filterByEnabled, "transform", request.id, listener)) {
                                return
                            }

                            // if HEAD request don't return the transform
                            val transformResponse = if (request.srcContext != null && !request.srcContext.fetchSource()) {
                                GetTransformResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, null)
                            } else {
                                GetTransformResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, transform)
                            }
                            listener.onResponse(transformResponse)
                        } catch (e: Exception) {
                            listener.onFailure(
                                OpenSearchStatusException(
                                    "Failed to parse transform",
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    ExceptionsHelper.unwrapCause(e)
                                )
                            )
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
