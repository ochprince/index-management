/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.interceptor

import com.colasoft.opensearch.cluster.metadata.IndexNameExpressionResolver
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.index.query.BoolQueryBuilder
import com.colasoft.opensearch.index.query.BoostingQueryBuilder
import com.colasoft.opensearch.index.query.ConstantScoreQueryBuilder
import com.colasoft.opensearch.index.query.DisMaxQueryBuilder
import com.colasoft.opensearch.index.query.MatchAllQueryBuilder
import com.colasoft.opensearch.index.query.MatchPhraseQueryBuilder
import com.colasoft.opensearch.index.query.QueryBuilder
import com.colasoft.opensearch.index.query.QueryStringQueryBuilder
import com.colasoft.opensearch.index.query.RangeQueryBuilder
import com.colasoft.opensearch.index.query.TermQueryBuilder
import com.colasoft.opensearch.index.query.TermsQueryBuilder
import com.colasoft.opensearch.index.search.MatchQuery
import com.colasoft.opensearch.indexmanagement.common.model.dimension.Dimension
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.rollup.model.RollupFieldMapping
import com.colasoft.opensearch.indexmanagement.rollup.model.RollupFieldMapping.Companion.UNKNOWN_MAPPING
import com.colasoft.opensearch.indexmanagement.rollup.query.QueryStringQueryUtil
import com.colasoft.opensearch.indexmanagement.rollup.settings.RollupSettings
import com.colasoft.opensearch.indexmanagement.rollup.util.getDateHistogram
import com.colasoft.opensearch.indexmanagement.rollup.util.getRollupJobs
import com.colasoft.opensearch.indexmanagement.rollup.util.isRollupIndex
import com.colasoft.opensearch.indexmanagement.rollup.util.populateFieldMappings
import com.colasoft.opensearch.indexmanagement.rollup.util.rewriteSearchSourceBuilder
import com.colasoft.opensearch.indexmanagement.util.IndexUtils
import com.colasoft.opensearch.search.aggregations.AggregationBuilder
import com.colasoft.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import com.colasoft.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval
import com.colasoft.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import com.colasoft.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import com.colasoft.opensearch.search.aggregations.metrics.AvgAggregationBuilder
import com.colasoft.opensearch.search.aggregations.metrics.MaxAggregationBuilder
import com.colasoft.opensearch.search.aggregations.metrics.MinAggregationBuilder
import com.colasoft.opensearch.search.aggregations.metrics.SumAggregationBuilder
import com.colasoft.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder
import com.colasoft.opensearch.search.internal.ShardSearchRequest
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportChannel
import com.colasoft.opensearch.transport.TransportInterceptor
import com.colasoft.opensearch.transport.TransportRequest
import com.colasoft.opensearch.transport.TransportRequestHandler

class RollupInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportInterceptor {

    @Volatile private var searchEnabled = RollupSettings.ROLLUP_SEARCH_ENABLED.get(settings)
    @Volatile private var searchAllJobs = RollupSettings.ROLLUP_SEARCH_ALL_JOBS.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(RollupSettings.ROLLUP_SEARCH_ENABLED) {
            searchEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(RollupSettings.ROLLUP_SEARCH_ALL_JOBS) {
            searchAllJobs = it
        }
    }

    @Suppress("SpreadOperator")
    override fun <T : TransportRequest> interceptHandler(
        action: String,
        executor: String,
        forceExecution: Boolean,
        actualHandler: TransportRequestHandler<T>
    ): TransportRequestHandler<T> {
        return object : TransportRequestHandler<T> {
            override fun messageReceived(request: T, channel: TransportChannel, task: Task) {
                if (searchEnabled && request is ShardSearchRequest) {
                    val index = request.shardId().indexName
                    val isRollupIndex = isRollupIndex(index, clusterService.state())
                    if (isRollupIndex) {
                        if (request.source().size() != 0) {
                            throw IllegalArgumentException("Rollup search must have size explicitly set to 0, but found ${request.source().size()}")
                        }

                        val indices = request.indices().map { it.toString() }.toTypedArray()
                        val concreteIndices = indexNameExpressionResolver
                            .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
                        // To extract fields from QueryStringQueryBuilder we need concrete source index name.
                        val rollupJob = clusterService.state().metadata.index(index).getRollupJobs()?.get(0)
                            ?: throw IllegalArgumentException("No rollup job associated with target_index")
                        val queryFieldMappings = getQueryMetadata(
                            request.source().query(),
                            IndexUtils.getConcreteIndex(rollupJob.sourceIndex, concreteIndices, clusterService.state())
                        )
                        val aggregationFieldMappings = getAggregationMetadata(request.source().aggregations()?.aggregatorFactories)
                        val fieldMappings = queryFieldMappings + aggregationFieldMappings

                        val allMatchingRollupJobs = validateIndicies(concreteIndices, fieldMappings)

                        // only rebuild if there is necessity to rebuild
                        if (fieldMappings.isNotEmpty()) {
                            rewriteShardSearchForRollupJobs(request, allMatchingRollupJobs)
                        }
                    }
                }
                actualHandler.messageReceived(request, channel, task)
            }
        }
    }
    /*
    * Validate that all indices have rollup job which matches field mappings from request
    * TODO return compiled list of issues here instead of just throwing exception
    * */
    private fun validateIndicies(concreteIndices: Array<String>, fieldMappings: Set<RollupFieldMapping>): Map<Rollup, Set<RollupFieldMapping>> {
        var allMatchingRollupJobs: Map<Rollup, Set<RollupFieldMapping>> = mapOf()
        for (concreteIndex in concreteIndices) {
            val rollupJobs = clusterService.state().metadata.index(concreteIndex).getRollupJobs()
                ?: throw IllegalArgumentException("Not all indices have rollup job")

            val (matchingRollupJobs, issues) = findMatchingRollupJobs(fieldMappings, rollupJobs)
            if (issues.isNotEmpty() || matchingRollupJobs.isEmpty()) {
                throw IllegalArgumentException("Could not find a rollup job that can answer this query because $issues")
            }
            allMatchingRollupJobs += matchingRollupJobs
        }
        return allMatchingRollupJobs
    }

    @Suppress("ComplexMethod")
    private fun getAggregationMetadata(
        aggregationBuilders: Collection<AggregationBuilder>?,
        fieldMappings: MutableSet<RollupFieldMapping> = mutableSetOf()
    ): Set<RollupFieldMapping> {
        aggregationBuilders?.forEach {
            when (it) {
                is TermsAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is DateHistogramAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is HistogramAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is SumAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is AvgAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is MaxAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is MinAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is ValueCountAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                else -> throw IllegalArgumentException("The ${it.type} aggregation is not currently supported in rollups")
            }
            if (it.subAggregations?.isNotEmpty() == true) {
                getAggregationMetadata(it.subAggregations, fieldMappings)
            }
        }
        return fieldMappings
    }

    @Suppress("ComplexMethod", "ThrowsCount")
    private fun getQueryMetadata(
        query: QueryBuilder?,
        concreteSourceIndexName: String,
        fieldMappings: MutableSet<RollupFieldMapping> = mutableSetOf()
    ): Set<RollupFieldMapping> {
        if (query == null) {
            return fieldMappings
        }

        when (query) {
            is TermQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), Dimension.Type.TERMS.type))
            }
            is TermsQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), Dimension.Type.TERMS.type))
            }
            is RangeQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), UNKNOWN_MAPPING))
            }
            is MatchAllQueryBuilder -> {
                // do nothing
            }
            is BoolQueryBuilder -> {
                query.must()?.forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
                query.mustNot()?.forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
                query.should()?.forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
                query.filter()?.forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
            }
            is BoostingQueryBuilder -> {
                this.getQueryMetadata(query.positiveQuery(), concreteSourceIndexName, fieldMappings)
                this.getQueryMetadata(query.negativeQuery(), concreteSourceIndexName, fieldMappings)
            }
            is ConstantScoreQueryBuilder -> {
                this.getQueryMetadata(query.innerQuery(), concreteSourceIndexName, fieldMappings)
            }
            is DisMaxQueryBuilder -> {
                query.innerQueries().forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
            }
            is MatchPhraseQueryBuilder -> {
                if (!query.analyzer().isNullOrEmpty() || query.slop() != MatchQuery.DEFAULT_PHRASE_SLOP ||
                    query.zeroTermsQuery() != MatchQuery.DEFAULT_ZERO_TERMS_QUERY
                ) {
                    throw IllegalArgumentException(
                        "The ${query.name} query is currently not supported with analyzer/slop/zero_terms_query in rollups"
                    )
                }
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), Dimension.Type.TERMS.type))
            }
            is QueryStringQueryBuilder -> {
                // Throws IllegalArgumentException if unable to parse query
                val (queryFields, otherFields) = QueryStringQueryUtil.extractFieldsFromQueryString(query, concreteSourceIndexName)
                for (field in queryFields) {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, field, Dimension.Type.TERMS.type))
                }
                for (field in otherFields.keys) {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, field, Dimension.Type.TERMS.type))
                }
            }
            else -> {
                throw IllegalArgumentException("The ${query.name} query is currently not supported in rollups")
            }
        }

        return fieldMappings
    }

    // TODO: How does this job matching work with roles/security?
    @Suppress("ComplexMethod")
    private fun findMatchingRollupJobs(
        fieldMappings: Set<RollupFieldMapping>,
        rollupJobs: List<Rollup>
    ): Pair<Map<Rollup, Set<RollupFieldMapping>>, Set<String>> {
        val rollupFieldMappings = rollupJobs.map { rollup ->
            rollup to rollup.populateFieldMappings()
        }.toMap()

        val knownFieldMappings = mutableSetOf<RollupFieldMapping>()
        val unknownFields = mutableSetOf<String>()

        fieldMappings.forEach {
            if (it.mappingType == UNKNOWN_MAPPING) unknownFields.add(it.fieldName)
            else knownFieldMappings.add(it)
        }

        val potentialRollupFieldMappings = rollupFieldMappings.filterValues {
            it.containsAll(knownFieldMappings) && it.map { rollupFieldMapping -> rollupFieldMapping.fieldName }.containsAll(unknownFields)
        }

        val issues = mutableSetOf<String>()
        if (potentialRollupFieldMappings.isEmpty()) {
            // create a global set of all field mappings
            val allFieldMappings = mutableSetOf<RollupFieldMapping>()
            rollupFieldMappings.values.forEach { allFieldMappings.addAll(it) }

            // create a global set of field names to handle unknown mapping types
            val allFields = allFieldMappings.map { it.fieldName }

            // Adding to the issue if cannot find defined field mapping or if the field is missing
            fieldMappings.forEach {
                if (!allFields.contains(it.fieldName)) issues.add(it.toIssue(true))
                else if (it.mappingType != UNKNOWN_MAPPING && !allFieldMappings.contains(it)) issues.add(it.toIssue())
            }
        }

        return potentialRollupFieldMappings to issues
    }

    // TODO: revisit - not entirely sure if this is the best thing to do, especially when there is a range query
    private fun pickRollupJob(rollups: Set<Rollup>): Rollup {
        if (rollups.size == 1) {
            return rollups.first()
        }
        // Make selection deterministic
        val sortedRollups = rollups.sortedBy { it.id }

        // Picking the job with largest rollup window for now
        return sortedRollups.reduce { matched, new ->
            if (getEstimateRollupInterval(matched) > getEstimateRollupInterval(new)) matched
            else new
        }
    }

    private fun getEstimateRollupInterval(rollup: Rollup): Long {
        return if (rollup.getDateHistogram().calendarInterval != null) {
            DateHistogramInterval(rollup.getDateHistogram().calendarInterval).estimateMillis()
        } else {
            DateHistogramInterval(rollup.getDateHistogram().fixedInterval).estimateMillis()
        }
    }

    private fun rewriteShardSearchForRollupJobs(request: ShardSearchRequest, matchingRollupJobs: Map<Rollup, Set<RollupFieldMapping>>) {
        val matchedRollup = pickRollupJob(matchingRollupJobs.keys)
        val fieldNameMappingTypeMap = matchingRollupJobs.getValue(matchedRollup).associateBy({ it.fieldName }, { it.mappingType })
        if (searchAllJobs) {
            request.source(request.source().rewriteSearchSourceBuilder(matchingRollupJobs.keys, fieldNameMappingTypeMap, matchedRollup.sourceIndex))
        } else {
            request.source(request.source().rewriteSearchSourceBuilder(matchedRollup, fieldNameMappingTypeMap, matchedRollup.sourceIndex))
        }
    }
}
