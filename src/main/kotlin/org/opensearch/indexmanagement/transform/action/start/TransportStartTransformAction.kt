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

package com.colasoft.opensearch.indexmanagement.transform.action.start

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.DocWriteResponse
import com.colasoft.opensearch.action.get.GetRequest
import com.colasoft.opensearch.action.get.GetResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.action.update.UpdateRequest
import com.colasoft.opensearch.action.update.UpdateResponse
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.xcontent.LoggingDeprecationHandler
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.common.xcontent.XContentHelper
import com.colasoft.opensearch.common.xcontent.XContentType
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.indexmanagement.transform.model.TransformMetadata
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService
import java.time.Instant

@Suppress("ReturnCount")
class TransportStartTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<StartTransformRequest, AcknowledgedResponse>(
    StartTransformAction.NAME, transportService, actionFilters, ::StartTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StartTransformRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id())
        val user = buildUser(client.threadPool().threadContext)
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                            return
                        }

                        val transform: Transform?
                        try {
                            transform = parseFromGetResponse(response, xContentRegistry, Transform.Companion::parse)
                        } catch (e: IllegalArgumentException) {
                            actionListener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                            return
                        }

                        if (!userHasPermissionForResource(user, transform.user, filterByEnabled, "transform", transform.id, actionListener)) {
                            return
                        }
                        if (transform.enabled) {
                            log.debug("Transform job is already enabled, checking if metadata needs to be updated")
                            return if (transform.metadataId == null) {
                                actionListener.onResponse(AcknowledgedResponse(true))
                            } else {
                                retrieveAndUpdateTransformMetadata(transform, actionListener)
                            }
                        }

                        updateTransformJob(transform, request, actionListener)
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    }
                }
            )
        }
    }

    private fun updateTransformJob(
        transform: Transform,
        request: StartTransformRequest,
        actionListener: ActionListener<AcknowledgedResponse>
    ) {
        val now = Instant.now().toEpochMilli()
        request.index(INDEX_MANAGEMENT_INDEX).doc(
            mapOf(
                Transform.TRANSFORM_TYPE to mapOf(
                    Transform.ENABLED_FIELD to true,
                    Transform.ENABLED_AT_FIELD to now, Transform.UPDATED_AT_FIELD to now
                )
            )
        )
        client.update(
            request,
            object : ActionListener<UpdateResponse> {
                override fun onResponse(response: UpdateResponse) {
                    if (response.result == DocWriteResponse.Result.UPDATED) {
                        // If there is a metadata ID on transform then we need to set it back to STARTED or RETRY
                        if (transform.metadataId != null) {
                            retrieveAndUpdateTransformMetadata(transform, actionListener)
                        } else {
                            actionListener.onResponse(AcknowledgedResponse(true))
                        }
                    } else {
                        actionListener.onResponse(AcknowledgedResponse(false))
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        )
    }

    private fun retrieveAndUpdateTransformMetadata(transform: Transform, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(INDEX_MANAGEMENT_INDEX, transform.metadataId).routing(transform.id)
        client.get(
            req,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists || response.isSourceEmpty) {
                        actionListener.onFailure(OpenSearchStatusException("Metadata doc missing for transform [${req.id()}]", RestStatus.NOT_FOUND))
                    } else {
                        val metadata = response.sourceAsBytesRef?.let {
                            val xcp = XContentHelper.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON
                            )
                            xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, TransformMetadata.Companion::parse)
                        }
                        if (metadata == null) {
                            actionListener.onFailure(
                                OpenSearchStatusException(
                                    "Metadata doc missing for transform [${req.id()}]", RestStatus.NOT_FOUND
                                )
                            )
                        } else {
                            updateTransformMetadata(transform, metadata, actionListener)
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        )
    }

    private fun updateTransformMetadata(transform: Transform, metadata: TransformMetadata, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus = when (metadata.status) {
            TransformMetadata.Status.FINISHED, TransformMetadata.Status.STOPPED -> TransformMetadata.Status.STARTED
            TransformMetadata.Status.STARTED, TransformMetadata.Status.INIT ->
                return actionListener.onResponse(AcknowledgedResponse(true))
            TransformMetadata.Status.FAILED -> TransformMetadata.Status.STARTED
        }
        val updateRequest = UpdateRequest(INDEX_MANAGEMENT_INDEX, transform.metadataId)
            .doc(
                mapOf(
                    TransformMetadata.TRANSFORM_METADATA_TYPE to mapOf(
                        TransformMetadata.STATUS_FIELD to updatedStatus.type,
                        TransformMetadata.FAILURE_REASON to null, TransformMetadata.LAST_UPDATED_AT_FIELD to now
                    )
                )
            )
            .routing(transform.id)
        client.update(
            updateRequest,
            object : ActionListener<UpdateResponse> {
                override fun onResponse(response: UpdateResponse) {
                    actionListener.onResponse(AcknowledgedResponse(response.result == DocWriteResponse.Result.UPDATED))
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        )
    }
}
