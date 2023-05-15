/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.index

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.DocWriteRequest
import com.colasoft.opensearch.action.admin.indices.mapping.get.GetMappingsAction
import com.colasoft.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import com.colasoft.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import com.colasoft.opensearch.action.get.GetRequest
import com.colasoft.opensearch.action.get.GetResponse
import com.colasoft.opensearch.action.index.IndexRequest
import com.colasoft.opensearch.action.index.IndexResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.action.support.IndicesOptions
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.metadata.IndexNameExpressionResolver
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.common.xcontent.XContentFactory.jsonBuilder
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.indexmanagement.IndexManagementIndices
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.transform.TransformValidator
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.indexmanagement.util.IndexUtils
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService

@Suppress("SpreadOperator", "LongParameterList")
class TransportIndexTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<IndexTransformRequest, IndexTransformResponse>(
    IndexTransformAction.NAME, transportService, actionFilters, ::IndexTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexTransformRequest, listener: ActionListener<IndexTransformResponse>) {
        IndexTransformHandler(client, listener, request).start()
    }

    inner class IndexTransformHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexTransformResponse>,
        private val request: IndexTransformRequest,
        private val user: User? = buildUser(client.threadPool().threadContext, request.transform.user)
    ) {

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            client.threadPool().threadContext.stashContext().use {
                if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                indexManagementIndices.checkAndUpdateIMConfigIndex(
                    ActionListener.wrap(::onConfigIndexAcknowledgedResponse, actionListener::onFailure)
                )
            }
        }

        private fun onConfigIndexAcknowledgedResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                if (request.opType() == DocWriteRequest.OpType.CREATE) {
                    validateAndPutTransform()
                } else {
                    updateTransform()
                }
            } else {
                val message = "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mappings."
                log.error(message)
                actionListener.onFailure(OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun updateTransform() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.transform.id)
            client.get(getRequest, ActionListener.wrap(::onGetTransform, actionListener::onFailure))
        }

        @Suppress("ReturnCount")
        private fun onGetTransform(response: GetResponse) {
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
            val modified = modifiedImmutableProperties(transform, request.transform)
            if (modified.isNotEmpty()) {
                return actionListener.onFailure(OpenSearchStatusException("Not allowed to modify $modified", RestStatus.BAD_REQUEST))
            }
            putTransform()
        }

        private fun modifiedImmutableProperties(transform: Transform, newTransform: Transform): List<String> {
            val modified = mutableListOf<String>()
            if (transform.sourceIndex != newTransform.sourceIndex) modified.add(Transform.SOURCE_INDEX_FIELD)
            if (transform.targetIndex != newTransform.targetIndex) modified.add(Transform.TARGET_INDEX_FIELD)
            if (transform.dataSelectionQuery != newTransform.dataSelectionQuery) modified.add(Transform.DATA_SELECTION_QUERY_FIELD)
            if (transform.groups != newTransform.groups) modified.add(Transform.GROUPS_FIELD)
            if (transform.aggregations != newTransform.aggregations) modified.add(Transform.AGGREGATIONS_FIELD)
            if (transform.continuous != newTransform.continuous) modified.add(Transform.CONTINUOUS_FIELD)
            return modified.toList()
        }

        private fun putTransform() {
            val transform = request.transform.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = this.user)
            request.index(INDEX_MANAGEMENT_INDEX)
                .id(request.transform.id)
                .source(transform.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            client.index(
                request,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        if (response.shardInfo.failed > 0) {
                            val failureReasons = response.shardInfo.failures.joinToString(",") { it.reason() }
                            actionListener.onFailure(OpenSearchStatusException(failureReasons, response.status()))
                        } else {
                            val status = if (request.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                            actionListener.onResponse(
                                IndexTransformResponse(
                                    response.id, response.version, response.seqNo, response.primaryTerm, status,
                                    transform.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm)
                                )
                            )
                        }
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
            )
        }

        private fun validateAndPutTransform() {
            val concreteIndices =
                indexNameExpressionResolver.concreteIndexNames(
                    clusterService.state(), IndicesOptions.lenientExpand(), true,
                    request.transform
                        .sourceIndex
                )
            if (concreteIndices.isEmpty()) {
                actionListener.onFailure(OpenSearchStatusException("No specified source index exist in the cluster", RestStatus.NOT_FOUND))
                return
            }

            val mappingRequest = GetMappingsRequest().indices(*concreteIndices)
            client.execute(
                GetMappingsAction.INSTANCE, mappingRequest,
                object : ActionListener<GetMappingsResponse> {
                    override fun onResponse(response: GetMappingsResponse) {
                        val issues = validateMappings(concreteIndices.toList(), response, request.transform)
                        if (issues.isNotEmpty()) {
                            val errorMessage = issues.joinToString(" ")
                            actionListener.onFailure(OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST))
                            return
                        }

                        putTransform()
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
            )
        }

        private fun validateMappings(indices: List<String>, response: GetMappingsResponse, transform: Transform): List<String> {
            val issues = mutableListOf<String>()
            indices.forEach { index ->
                issues.addAll(TransformValidator.validateMappingsResponse(index, response, transform))
            }

            return issues
        }
    }
}
