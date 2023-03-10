/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.cluster.ClusterState
import com.colasoft.opensearch.cluster.ClusterStateTaskConfig
import com.colasoft.opensearch.cluster.ClusterStateTaskExecutor
import com.colasoft.opensearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult
import com.colasoft.opensearch.cluster.ClusterStateTaskListener
import com.colasoft.opensearch.cluster.block.ClusterBlockException
import com.colasoft.opensearch.cluster.block.ClusterBlockLevel
import com.colasoft.opensearch.cluster.metadata.IndexMetadata
import com.colasoft.opensearch.cluster.metadata.IndexNameExpressionResolver
import com.colasoft.opensearch.cluster.metadata.Metadata
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.Priority
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.index.Index
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.threadpool.ThreadPool
import com.colasoft.opensearch.transport.TransportService

class TransportUpdateManagedIndexMetaDataAction @Inject constructor(
    threadPool: ThreadPool,
    clusterService: ClusterService,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val indexMetadataProvider: IndexMetadataProvider,
    indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportClusterManagerNodeAction<UpdateManagedIndexMetaDataRequest, AcknowledgedResponse>(
    UpdateManagedIndexMetaDataAction.INSTANCE.name(),
    transportService,
    clusterService,
    threadPool,
    actionFilters,
    Writeable.Reader { UpdateManagedIndexMetaDataRequest(it) },
    indexNameExpressionResolver
) {

    private val log = LogManager.getLogger(javaClass)
    private val executor = ManagedIndexMetaDataExecutor()

    override fun checkBlock(request: UpdateManagedIndexMetaDataRequest, state: ClusterState): ClusterBlockException? {
        // https://github.com/elastic/elasticsearch/commit/ae14b4e6f96b554ca8f4aaf4039b468f52df0123
        // This commit will help us to give each individual index name and the error that is cause it. For now it will be a generic error message.
        val indicesToAddTo = request.indicesToAddManagedIndexMetaDataTo.map { it.first }.toTypedArray()
        val indicesToRemoveFrom = request.indicesToRemoveManagedIndexMetaDataFrom.map { it }.toTypedArray()
        val indices = checkExtensionsOverrideBlock(indicesToAddTo + indicesToRemoveFrom, state)

        return state.blocks.indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices)
    }

    /*
     * Index Management extensions may provide an index setting, which, if set to true, overrides the cluster metadata write block
     */
    private fun checkExtensionsOverrideBlock(indices: Array<Index>, state: ClusterState): Array<String> {
        val indexBlockOverrideSettings = indexMetadataProvider.getIndexMetadataWriteOverrideSettings()
        val indicesToBlock = indices.toMutableList()
        indexBlockOverrideSettings.forEach { indexBlockOverrideSetting ->
            indicesToBlock.removeIf { state.metadata.getIndexSafe(it).settings.getAsBoolean(indexBlockOverrideSetting, false) }
        }
        return indicesToBlock
            .map { it.name }
            .toTypedArray()
    }

    override fun masterOperation(
        request: UpdateManagedIndexMetaDataRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        clusterService.submitStateUpdateTask(
            IndexManagementPlugin.OLD_PLUGIN_NAME,
            ManagedIndexMetaDataTask(request.indicesToAddManagedIndexMetaDataTo, request.indicesToRemoveManagedIndexMetaDataFrom),
            ClusterStateTaskConfig.build(Priority.NORMAL),
            executor,
            object : ClusterStateTaskListener {
                override fun onFailure(source: String, e: Exception) = listener.onFailure(e)

                override fun clusterStateProcessed(source: String, oldState: ClusterState, newState: ClusterState) =
                    listener.onResponse(AcknowledgedResponse(true))
            }
        )
    }

    override fun read(si: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(si)
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    inner class ManagedIndexMetaDataExecutor : ClusterStateTaskExecutor<ManagedIndexMetaDataTask> {

        override fun execute(currentState: ClusterState, tasks: List<ManagedIndexMetaDataTask>): ClusterTasksResult<ManagedIndexMetaDataTask> {
            val newClusterState = getUpdatedClusterState(currentState, tasks)
            return ClusterTasksResult.builder<ManagedIndexMetaDataTask>().successes(tasks).build(newClusterState)
        }
    }

    fun getUpdatedClusterState(currentState: ClusterState, tasks: List<ManagedIndexMetaDataTask>): ClusterState {
        // If there are no indices to make changes to, return early.
        // Also doing this because when creating a metaDataBuilder and making no changes to it, for some
        // reason the task does not complete, leading to indefinite suspension.
        if (tasks.all { it.indicesToAddManagedIndexMetaDataTo.isEmpty() && it.indicesToRemoveManagedIndexMetaDataFrom.isEmpty() }
        ) {
            return currentState
        }
        log.trace("Start of building new cluster state")
        val metaDataBuilder = Metadata.builder(currentState.metadata)
        for (task in tasks) {
            for (pair in task.indicesToAddManagedIndexMetaDataTo) {
                if (currentState.metadata.hasIndex(pair.first.name)) {
                    metaDataBuilder.put(
                        IndexMetadata.builder(currentState.metadata.index(pair.first))
                            .putCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE, pair.second.toMap())
                    )
                } else {
                    log.debug("No IndexMetadata found for [${pair.first.name}] when updating ManagedIndexMetaData")
                }
            }

            for (index in task.indicesToRemoveManagedIndexMetaDataFrom) {
                if (currentState.metadata.hasIndex(index.name)) {
                    val indexMetaDataBuilder = IndexMetadata.builder(currentState.metadata.index(index))
                    indexMetaDataBuilder.removeCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE)

                    metaDataBuilder.put(indexMetaDataBuilder)
                } else {
                    log.debug("No IndexMetadata found for [${index.name}] when removing ManagedIndexMetaData")
                }
            }
        }
        log.trace("End of building new cluster state")

        return ClusterState.builder(currentState).metadata(metaDataBuilder).build()
    }

    companion object {
        data class ManagedIndexMetaDataTask(
            val indicesToAddManagedIndexMetaDataTo: List<Pair<Index, ManagedIndexMetaData>>,
            val indicesToRemoveManagedIndexMetaDataFrom: List<Index>
        )
    }
}
