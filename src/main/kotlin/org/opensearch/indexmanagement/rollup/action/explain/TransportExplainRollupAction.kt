/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.explain

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.ResourceNotFoundException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.search.SearchRequest
import com.colasoft.opensearch.action.search.SearchResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.index.query.BoolQueryBuilder
import com.colasoft.opensearch.index.query.IdsQueryBuilder
import com.colasoft.opensearch.index.query.WildcardQueryBuilder
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import com.colasoft.opensearch.indexmanagement.opensearchapi.contentParser
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.rollup.model.ExplainRollup
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.rollup.model.RollupMetadata
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.search.builder.SearchSourceBuilder
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.RemoteTransportException
import com.colasoft.opensearch.transport.TransportService
import kotlin.Exception

class TransportExplainRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters
) : HandledTransportAction<ExplainRollupRequest, ExplainRollupResponse>(
    ExplainRollupAction.NAME, transportService, actionFilters, ::ExplainRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: ExplainRollupRequest, actionListener: ActionListener<ExplainRollupResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val ids = request.rollupIDs
        // Instantiate concrete ids to metadata map by removing wildcard matches
        val idsToExplain: MutableMap<String, ExplainRollup?> = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
        // First search is for all rollup documents that match at least one of the given rollupIDs
        val queryBuilder = BoolQueryBuilder().minimumShouldMatch(1).apply {
            ids.forEach {
                this.should(WildcardQueryBuilder("${Rollup.ROLLUP_TYPE}.${Rollup.ROLLUP_ID_FIELD}.keyword", "*$it*"))
            }
        }
        val user = buildUser(client.threadPool().threadContext)
        addUserFilter(user, queryBuilder, filterByEnabled, "rollup.user")

        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().size(MAX_HITS).query(queryBuilder))
        client.threadPool().threadContext.stashContext().use {
            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        try {
                            response.hits.hits.forEach {
                                val rollup = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, Rollup.Companion::parse)
                                idsToExplain[rollup.id] = ExplainRollup(metadataID = rollup.metadataID)
                            }
                        } catch (e: Exception) {
                            log.error("Failed to parse explain response", e)
                            actionListener.onFailure(e)
                            return
                        }

                        val metadataIds = idsToExplain.values.mapNotNull { it?.metadataID }
                        val metadataSearchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX)
                            .source(SearchSourceBuilder().size(MAX_HITS).query(IdsQueryBuilder().addIds(*metadataIds.toTypedArray())))
                        client.search(
                            metadataSearchRequest,
                            object : ActionListener<SearchResponse> {
                                override fun onResponse(response: SearchResponse) {
                                    try {
                                        response.hits.hits.forEach {
                                            val metadata = contentParser(it.sourceRef)
                                                .parseWithType(it.id, it.seqNo, it.primaryTerm, RollupMetadata.Companion::parse)
                                            idsToExplain.computeIfPresent(metadata.rollupID) { _,
                                                explainRollup ->
                                                explainRollup.copy(metadata = metadata)
                                            }
                                        }
                                        actionListener.onResponse(ExplainRollupResponse(idsToExplain.toMap()))
                                    } catch (e: Exception) {
                                        log.error("Failed to parse rollup metadata", e)
                                        actionListener.onFailure(e)
                                        return
                                    }
                                }

                                override fun onFailure(e: Exception) {
                                    log.error("Failed to search rollup metadata", e)
                                    when (e) {
                                        is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                                        else -> actionListener.onFailure(e)
                                    }
                                }
                            }
                        )
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Failed to search for rollups", e)
                        when (e) {
                            is ResourceNotFoundException -> {
                                val nonWildcardIds = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
                                actionListener.onResponse(ExplainRollupResponse(nonWildcardIds))
                            }
                            is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                            else -> actionListener.onFailure(e)
                        }
                    }
                }
            )
        }
    }
}
