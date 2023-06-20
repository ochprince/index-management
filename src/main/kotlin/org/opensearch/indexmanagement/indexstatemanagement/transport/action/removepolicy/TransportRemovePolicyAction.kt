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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.OpenSearchSecurityException
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.admin.cluster.state.ClusterStateRequest
import com.colasoft.opensearch.action.admin.cluster.state.ClusterStateResponse
import com.colasoft.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import com.colasoft.opensearch.action.bulk.BulkRequest
import com.colasoft.opensearch.action.bulk.BulkResponse
import com.colasoft.opensearch.action.get.MultiGetRequest
import com.colasoft.opensearch.action.get.MultiGetResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.action.support.IndicesOptions
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.cluster.block.ClusterBlockException
import com.colasoft.opensearch.cluster.metadata.IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING
import com.colasoft.opensearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_SETTING
import com.colasoft.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY
import com.colasoft.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.index.Index
import com.colasoft.opensearch.index.IndexNotFoundException
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getUuidsForClosedIndices
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexMetadataRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.removeClusterStateMetadatas
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata
import com.colasoft.opensearch.indexmanagement.util.IndexManagementException
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService

@Suppress("SpreadOperator")
class TransportRemovePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val indexMetadataProvider: IndexMetadataProvider
) : HandledTransportAction<RemovePolicyRequest, ISMStatusResponse>(
    RemovePolicyAction.NAME, transportService, actionFilters, ::RemovePolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: RemovePolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        RemovePolicyHandler(client, listener, request).start()
    }

    inner class RemovePolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: RemovePolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {

        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val indicesToRemove = mutableMapOf<String, String>() // uuid: name
        private val indicesWithAutoManageFalseBlock = mutableSetOf<String>()
        private val indicesWithReadOnlyBlock = mutableSetOf<String>()
        private val indicesWithReadOnlyAllowDeleteBlock = mutableSetOf<String>()

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            if (user == null) {
                getIndicesToRemove()
            } else {
                validateAndGetIndices()
            }
        }

        private fun validateAndGetIndices() {
            val managedIndexRequest = ManagedIndexRequest().indices(*request.indices.toTypedArray())
            client.execute(
                ManagedIndexAction.INSTANCE,
                managedIndexRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        getIndicesToRemove()
                    }

                    override fun onFailure(e: java.lang.Exception) {
                        actionListener.onFailure(
                            IndexManagementException.wrap(
                                when (e is OpenSearchSecurityException) {
                                    true -> OpenSearchStatusException(
                                        "User doesn't have required index permissions on one or more requested indices: ${e.localizedMessage}",
                                        RestStatus.FORBIDDEN
                                    )
                                    false -> e
                                }
                            )
                        )
                    }
                }
            )
        }

        private fun getIndicesToRemove() {
            CoroutineScope(Dispatchers.IO).launch {
                val indexNameToMetadata: MutableMap<String, ISMIndexMetadata> = HashMap()
                try {
                    indexNameToMetadata.putAll(indexMetadataProvider.getISMIndexMetadataByType(request.indexType, request.indices))
                } catch (e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    return@launch
                }
                indexNameToMetadata.forEach { (indexName, indexMetadata) ->
                    indicesToRemove.putIfAbsent(indexMetadata.indexUuid, indexName)
                }
                if (request.indexType == DEFAULT_INDEX_TYPE) {
                    getClusterState()
                } else {
                    getExistingManagedIndices()
                }
            }
        }

        private fun getClusterState() {
            val strictExpandOptions = IndicesOptions.strictExpand()

            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .local(false)
                .indicesOptions(strictExpandOptions)

            client.threadPool().threadContext.stashContext().use {
                client.admin()
                    .cluster()
                    .state(
                        clusterStateRequest,
                        object : ActionListener<ClusterStateResponse> {
                            override fun onResponse(response: ClusterStateResponse) {
                                val indexMetadatas = response.state.metadata.indices
                                indexMetadatas.forEach {
                                    if (it.value.settings.get(ManagedIndexSettings.AUTO_MANAGE.key) == "false") {
                                        indicesWithAutoManageFalseBlock.add(it.value.indexUUID)
                                    }
                                    if (it.value.settings.get(SETTING_READ_ONLY) == "true") {
                                        indicesWithReadOnlyBlock.add(it.value.indexUUID)
                                    }
                                    if (it.value.settings.get(SETTING_READ_ONLY_ALLOW_DELETE) == "true") {
                                        indicesWithReadOnlyAllowDeleteBlock.add(it.value.indexUUID)
                                    }
                                }

                                val defaultIndexMetadataService = indexMetadataProvider.services[DEFAULT_INDEX_TYPE] as DefaultIndexMetadataService
                                getUuidsForClosedIndices(response.state, defaultIndexMetadataService).forEach {
                                    failedIndices.add(FailedIndex(indicesToRemove[it] as String, it, "This index is closed"))
                                    indicesToRemove.remove(it)
                                }

                                getExistingManagedIndices()
                            }

                            override fun onFailure(t: Exception) {
                                actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                            }
                        }
                    )
            }
        }

        private fun getExistingManagedIndices() {
            if (indicesToRemove.isEmpty()) {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                return
            }

            val multiGetReq = MultiGetRequest()
            indicesToRemove.forEach { multiGetReq.add(INDEX_MANAGEMENT_INDEX, it.key) }

            client.multiGet(
                multiGetReq,
                object : ActionListener<MultiGetResponse> {
                    override fun onResponse(response: MultiGetResponse) {
                        // config index may not be initialized
                        val f = response.responses.first()
                        if (f.isFailed && f.failure.failure is IndexNotFoundException) {
                            indicesToRemove.forEach { (uuid, name) ->
                                failedIndices.add(
                                    FailedIndex(
                                        name,
                                        uuid,
                                        "This index does not have a policy to remove"
                                    )
                                )
                            }
                            actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                            return
                        }

                        response.forEach {
                            if (!it.response.isExists) {
                                val docId = it.id // docId is managed index uuid
                                failedIndices.add(
                                    FailedIndex(
                                        indicesToRemove[docId] as String, docId,
                                        "This index does not have a policy to remove"
                                    )
                                )
                                indicesToRemove.remove(docId)
                            }
                        }

                        if (request.indexType == DEFAULT_INDEX_TYPE) {
                            updateSettings(indicesToRemove)
                        } else {
                            removeManagedIndices()
                        }
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        /**
         * try to update auto_manage setting to false before delete managed-index
         * so that index will not be picked up by Coordinator background sweep process
         * this wont happen for cold indices
         * if update setting failed, remove managed-index and metadata will not happen
         */
        @Suppress("SpreadOperator")
        fun updateSettings(indices: Map<String, String>) {
            // indices divide to read_only, read_only_allow_delete, normal
            val indicesUuidsSet = indices.keys.toSet() - indicesWithAutoManageFalseBlock
            val readOnlyIndices = indicesUuidsSet.filter { it in indicesWithReadOnlyBlock }
            val readOnlyAllowDeleteIndices = (indicesUuidsSet - readOnlyIndices).filter { it in indicesWithReadOnlyAllowDeleteBlock }
            val normalIndices = indicesUuidsSet - readOnlyIndices - readOnlyAllowDeleteIndices

            val updateSettingReqsList = mutableListOf<UpdateSettingsRequest>()
            if (readOnlyIndices.isNotEmpty()) {
                updateSettingReqsList.add(
                    UpdateSettingsRequest().indices(*readOnlyIndices.map { indices[it] }.toTypedArray())
                        .settings(
                            Settings.builder().put(ManagedIndexSettings.AUTO_MANAGE.key, false)
                                .put(INDEX_READ_ONLY_SETTING.key, true)
                        )
                )
            }
            if (readOnlyAllowDeleteIndices.isNotEmpty()) {
                updateSettingReqsList.add(
                    UpdateSettingsRequest().indices(*readOnlyAllowDeleteIndices.map { indices[it] }.toTypedArray())
                        .settings(
                            Settings.builder().put(ManagedIndexSettings.AUTO_MANAGE.key, false)
                                .put(INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.key, true)
                        )
                )
            }
            if (normalIndices.isNotEmpty()) {
                updateSettingReqsList.add(
                    UpdateSettingsRequest().indices(*normalIndices.map { indices[it] }.toTypedArray())
                        .settings(Settings.builder().put(ManagedIndexSettings.AUTO_MANAGE.key, false))
                )
            }

            updateSettingCallChain(0, updateSettingReqsList)
        }

        fun updateSettingCallChain(current: Int, updateSettingReqsList: List<UpdateSettingsRequest>) {
            if (updateSettingReqsList.isEmpty()) {
                removeManagedIndices()
                return
            }
            client.admin().indices().updateSettings(
                updateSettingReqsList[current],
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        if (!response.isAcknowledged) {
                            actionListener.onFailure(
                                IndexManagementException.wrap(
                                    Exception("Failed to remove policy because ISM auto_manage setting update requests are not fully acknowledged.")
                                )
                            )
                            return
                        }
                        if (current < updateSettingReqsList.size - 1) {
                            updateSettingCallChain(current + 1, updateSettingReqsList)
                        } else {
                            removeManagedIndices()
                        }
                    }

                    override fun onFailure(t: Exception) {
                        val ex = ExceptionsHelper.unwrapCause(t) as Exception
                        actionListener.onFailure(
                            IndexManagementException.wrap(
                                Exception("Failed to remove policy because ISM auto_manage setting update requests failed with exception:", ex)
                            )
                        )
                    }
                }
            )
        }

        @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
        fun removeManagedIndices() {
            if (indicesToRemove.isNotEmpty()) {
                val bulkReq = BulkRequest()
                indicesToRemove.forEach { bulkReq.add(deleteManagedIndexRequest(it.key)) }
                client.bulk(
                    bulkReq,
                    object : ActionListener<BulkResponse> {
                        override fun onResponse(response: BulkResponse) {
                            response.forEach {
                                val docId = it.id // docId is indexUuid of the managed index
                                if (it.isFailed) {
                                    failedIndices.add(
                                        FailedIndex(
                                            indicesToRemove[docId] as String,
                                            docId,
                                            "Failed to remove policy"
                                        )
                                    )
                                    indicesToRemove.remove(docId)
                                }
                            }

                            // clean metadata for indicesToRemove
                            val indicesToRemoveMetadata = indicesToRemove.map { Index(it.value, it.key) }
                            // best effort
                            CoroutineScope(Dispatchers.IO).launch { removeClusterStateMetadatas(client, log, indicesToRemoveMetadata) }
                            removeMetadatas(indicesToRemoveMetadata)
                        }

                        override fun onFailure(t: Exception) {
                            if (t is ClusterBlockException) {
                                indicesToRemove.forEach { (uuid, name) ->
                                    failedIndices.add(
                                        FailedIndex(
                                            name, uuid,
                                            "Failed to remove policy due to ClusterBlockingException: ${t.message}"
                                        )
                                    )
                                }
                                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                            } else {
                                actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                            }
                        }
                    }
                )
            } else {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
            }
        }

        fun removeMetadatas(indices: List<Index>) {
            val request = indices.map { deleteManagedIndexMetadataRequest(it.uuid) }
            val bulkReq = BulkRequest().add(request)
            client.bulk(
                bulkReq,
                object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        response.forEach {
                            val docId = it.id
                            if (it.isFailed) {
                                failedIndices.add(
                                    FailedIndex(
                                        indicesToRemove[docId] as String, docId,
                                        "Failed to clean metadata due to: ${it.failureMessage}"
                                    )
                                )
                                indicesToRemove.remove(docId)
                            }
                        }
                        actionListener.onResponse(ISMStatusResponse(indicesToRemove.size, failedIndices))
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(
                            IndexManagementException.wrap(
                                Exception("Failed to clean metadata for remove policy indices.", e)
                            )
                        )
                    }
                }
            )
        }
    }
}
