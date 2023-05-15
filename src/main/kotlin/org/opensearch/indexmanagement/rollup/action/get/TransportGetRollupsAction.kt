/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.get

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.search.SearchRequest
import com.colasoft.opensearch.action.search.SearchResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.commons.ConfigConstants
import com.colasoft.opensearch.index.query.BoolQueryBuilder
import com.colasoft.opensearch.index.query.ExistsQueryBuilder
import com.colasoft.opensearch.index.query.WildcardQueryBuilder
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.opensearchapi.contentParser
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.search.builder.SearchSourceBuilder
import com.colasoft.opensearch.search.sort.SortOrder
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService
import kotlin.Exception

class TransportGetRollupsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetRollupsRequest, GetRollupsResponse> (
    GetRollupsAction.NAME, transportService, actionFilters, ::GetRollupsRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: GetRollupsRequest, listener: ActionListener<GetRollupsResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val searchString = request.searchString.trim()
        val from = request.from
        val size = request.size
        val sortField = request.sortField
        val sortDirection = request.sortDirection
        // TODO: Allow filtering for [continuous, job state, metadata status, targetindex, sourceindex]
        val boolQueryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(Rollup.ROLLUP_TYPE))
        if (searchString.isNotEmpty()) {
            boolQueryBuilder.filter(WildcardQueryBuilder("${Rollup.ROLLUP_TYPE}.${Rollup.ROLLUP_ID_FIELD}.keyword", "*$searchString*"))
        }
        val user = buildUser(client.threadPool().threadContext)
        addUserFilter(user, boolQueryBuilder, filterByEnabled, "rollup.user")
        val searchSourceBuilder = SearchSourceBuilder().query(boolQueryBuilder).from(from).size(size).seqNoAndPrimaryTerm(true)
            .sort(sortField, SortOrder.fromString(sortDirection))
        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
        client.threadPool().threadContext.stashContext().use {
            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        val totalRollups = response.hits.totalHits?.value ?: 0

                        if (response.shardFailures.isNotEmpty()) {
                            val failure = response.shardFailures.reduce { s1, s2 -> if (s1.status().status > s2.status().status) s1 else s2 }
                            listener.onFailure(OpenSearchStatusException("Get rollups failed on some shards", failure.status(), failure.cause))
                        } else {
                            try {
                                val rollups = response.hits.hits.map {
                                    contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, Rollup.Companion::parse)
                                }
                                listener.onResponse(GetRollupsResponse(rollups, totalRollups.toInt(), RestStatus.OK))
                            } catch (e: Exception) {
                                listener.onFailure(
                                    OpenSearchStatusException(
                                        "Failed to parse rollups",
                                        RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e)
                                    )
                                )
                            }
                        }
                    }

                    override fun onFailure(e: Exception) = listener.onFailure(e)
                }
            )
        }
    }
}
