/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.util

import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.ActionResponse
import com.colasoft.opensearch.action.search.SearchRequest
import com.colasoft.opensearch.action.search.SearchResponse
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.common.bytes.BytesReference
import com.colasoft.opensearch.common.xcontent.LoggingDeprecationHandler
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.common.xcontent.XContentHelper
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentType
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsResponse
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsResponse
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.search.builder.SearchSourceBuilder

fun getJobs(
    client: Client,
    searchSourceBuilder: SearchSourceBuilder,
    listener: ActionListener<ActionResponse>,
    scheduledJobType: String,
    contentParser: (b: BytesReference) -> XContentParser = ::contentParser
) {
    val searchRequest = SearchRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    client.search(
        searchRequest,
        object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                val totalJobs = response.hits.totalHits?.value ?: 0

                if (response.shardFailures.isNotEmpty()) {
                    val failure = response.shardFailures.reduce { s1, s2 -> if (s1.status().status > s2.status().status) s1 else s2 }
                    listener.onFailure(OpenSearchStatusException("Get $scheduledJobType failed on some shards", failure.status(), failure.cause))
                } else {
                    try {
                        val jobs = response.hits.hits.map {
                            contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, getParser(scheduledJobType))
                        }
                        listener.onResponse(populateResponse(scheduledJobType, jobs, RestStatus.OK, totalJobs.toInt()))
                    } catch (e: Exception) {
                        listener.onFailure(
                            OpenSearchStatusException(
                                "Failed to parse $scheduledJobType",
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

@Suppress("UNCHECKED_CAST")
private fun populateResponse(
    jobType: String,
    jobs: List<Any>,
    status: RestStatus,
    totalJobs: Int
): ActionResponse {
    return when (jobType) {
        Rollup.ROLLUP_TYPE -> GetRollupsResponse(jobs as List<Rollup>, totalJobs, status)
        Transform.TRANSFORM_TYPE -> GetTransformsResponse(jobs as List<Transform>, totalJobs, status)
        else -> {
            throw OpenSearchStatusException("Unknown scheduled job type", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }
}

private fun getParser(jobType: String): (XContentParser, String, Long, Long) -> Any {
    return when (jobType) {
        Transform.TRANSFORM_TYPE -> Transform.Companion::parse
        Rollup.ROLLUP_TYPE -> Rollup.Companion::parse
        else -> {
            throw OpenSearchStatusException("Unknown scheduled job type", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }
}

private fun contentParser(bytesReference: BytesReference): XContentParser {
    return XContentHelper.createParser(
        NamedXContentRegistry.EMPTY,
        LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON
    )
}
