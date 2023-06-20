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

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.explain

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.search.SearchRequest
import com.colasoft.opensearch.action.search.SearchResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.util.concurrent.ThreadContext
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.index.IndexNotFoundException
import com.colasoft.opensearch.index.query.BoolQueryBuilder
import com.colasoft.opensearch.index.query.ExistsQueryBuilder
import com.colasoft.opensearch.index.query.TermQueryBuilder
import com.colasoft.opensearch.index.query.WildcardQueryBuilder
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import com.colasoft.opensearch.indexmanagement.opensearchapi.contentParser
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.opensearchapi.suspendUntil
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.ExplainSMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.SM_METADATA_TYPE
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.ENABLED_FIELD
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.NAME_FIELD
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.smMetadataDocIdToPolicyName
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.search.builder.SearchSourceBuilder
import com.colasoft.opensearch.search.fetch.subphase.FetchSourceContext
import com.colasoft.opensearch.transport.TransportService

class TransportExplainSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<ExplainSMPolicyRequest, ExplainSMPolicyResponse>(
    SMActions.EXPLAIN_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::ExplainSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: ExplainSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): ExplainSMPolicyResponse {
        val policyNames = request.policyNames.toSet()

        val namesToEnabled = getPolicyEnabledStatus(policyNames, user)
        val namesToMetadata = getSMMetadata(namesToEnabled.keys)
        return buildExplainResponse(namesToEnabled, namesToMetadata)
    }

    @Suppress("ThrowsCount")
    private suspend fun getPolicyEnabledStatus(policyNames: Set<String>, user: User?): Map<String, Boolean> {
        // Search the config index for SM policies
        val searchRequest = getPolicyEnabledSearchRequest(policyNames, user)
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        } catch (e: Exception) {
            log.error("Failed to search for snapshot management policy", e)
            throw OpenSearchStatusException("Failed to search for snapshot management policy", RestStatus.INTERNAL_SERVER_ERROR)
        }

        // Parse each returned policy to get the job enabled status
        return try {
            searchResponse.hits.hits.associate {
                parseNameToEnabled(contentParser(it.sourceRef))
            }
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management policy in search response", e)
            throw OpenSearchStatusException("Failed to parse snapshot management policy", RestStatus.NOT_FOUND)
        }
    }

    private fun getPolicyEnabledSearchRequest(policyNames: Set<String>, user: User?): SearchRequest {
        val queryBuilder = getPolicyQuery(policyNames)

        // Add user filter if enabled
        SecurityUtils.addUserFilter(user, queryBuilder, filterByEnabled, "sm_policy.user")

        // Only return the name and enabled field
        val includes = arrayOf(
            "${SMPolicy.SM_TYPE}.$NAME_FIELD",
            "${SMPolicy.SM_TYPE}.$ENABLED_FIELD"
        )
        val fetchSourceContext = FetchSourceContext(true, includes, arrayOf())
        val searchSourceBuilder = SearchSourceBuilder().size(MAX_HITS).query(queryBuilder).fetchSource(fetchSourceContext)
        return SearchRequest(INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    }

    private fun getPolicyQuery(policyNames: Set<String>): BoolQueryBuilder {
        // Search for all SM Policy documents which match at least one of the given names
        val queryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(SMPolicy.SM_TYPE))
        queryBuilder.minimumShouldMatch(1).apply {
            policyNames.forEach { policyName ->
                if (policyName.contains('*') || policyName.contains('?')) {
                    this.should(WildcardQueryBuilder("${SMPolicy.SM_TYPE}.$NAME_FIELD", policyName))
                } else {
                    this.should(TermQueryBuilder("${SMPolicy.SM_TYPE}.$NAME_FIELD", policyName))
                }
            }
        }
        return queryBuilder
    }

    private suspend fun getSMMetadata(policyNames: Set<String>): Map<String, SMMetadata> {
        val searchRequest = getSMMetadataSearchRequest(policyNames)
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }

        return try {
            searchResponse.hits.hits.associate {
                val smMetadata = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, SMMetadata.Companion::parse)
                smMetadataDocIdToPolicyName(smMetadata.id) to smMetadata
            }
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management metadata in search response", e)
            throw OpenSearchStatusException("Failed to parse snapshot management metadata", RestStatus.NOT_FOUND)
        }
    }

    private fun getSMMetadataSearchRequest(policyNames: Set<String>): SearchRequest {
        // Search for all SM Metadata documents which match at least one of the given names
        val queryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(SM_METADATA_TYPE))
        queryBuilder.minimumShouldMatch(1).apply {
            policyNames.forEach {
                this.should(TermQueryBuilder("$SM_METADATA_TYPE.$NAME_FIELD", it))
            }
        }

        // Search the config index for SM Metadata
        return SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().size(MAX_HITS).query(queryBuilder))
    }

    private fun parseNameToEnabled(xcp: XContentParser): Pair<String, Boolean> {
        var name: String? = null
        var enabled: Boolean? = null

        if (xcp.currentToken() == null) xcp.nextToken()
        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                NAME_FIELD -> name = xcp.text()
                ENABLED_FIELD -> enabled = xcp.booleanValue()
            }
        }
        return requireNotNull(name) { "The name field of SMPolicy must not be null." } to
            requireNotNull(enabled) { "The enabled field of SMPolicy must not be null." }
    }

    private fun buildExplainResponse(namesToEnabled: Map<String, Boolean>, namesToMetadata: Map<String, SMMetadata>): ExplainSMPolicyResponse {
        val policiesToExplain = namesToEnabled.entries.associate { (policyName, enabled) ->
            policyName to ExplainSMPolicy(namesToMetadata[policyName], enabled)
        }
        log.debug("Explain response: $policiesToExplain")
        return ExplainSMPolicyResponse(policiesToExplain)
    }
}
