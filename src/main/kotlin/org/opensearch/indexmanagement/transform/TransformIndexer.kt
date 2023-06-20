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

package com.colasoft.opensearch.indexmanagement.transform

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.OpenSearchSecurityException
import com.colasoft.opensearch.action.DocWriteRequest
import com.colasoft.opensearch.action.admin.indices.create.CreateIndexRequest
import com.colasoft.opensearch.action.admin.indices.create.CreateIndexResponse
import com.colasoft.opensearch.action.bulk.BackoffPolicy
import com.colasoft.opensearch.action.bulk.BulkItemResponse
import com.colasoft.opensearch.action.bulk.BulkRequest
import com.colasoft.opensearch.action.bulk.BulkResponse
import com.colasoft.opensearch.action.index.IndexRequest
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.IndexManagementIndices
import com.colasoft.opensearch.indexmanagement.opensearchapi.retry
import com.colasoft.opensearch.indexmanagement.opensearchapi.suspendUntil
import com.colasoft.opensearch.indexmanagement.transform.exceptions.TransformIndexException
import com.colasoft.opensearch.indexmanagement.transform.settings.TransformSettings
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.transport.RemoteTransportException

@Suppress("ComplexMethod")
class TransformIndexer(
    settings: Settings,
    private val clusterService: ClusterService,
    private val client: Client
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var backoffPolicy = BackoffPolicy.constantBackoff(
        TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS.get(settings),
        TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT.get(settings)
    )

    init {
        // To update the retry policy with updated settings
        clusterService.clusterSettings.addSettingsUpdateConsumer(
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS,
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT
        ) { millis, count ->
            backoffPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    private suspend fun createTargetIndex(index: String) {
        if (!clusterService.state().routingTable.hasIndex(index)) {
            val request = CreateIndexRequest(index)
                .mapping(IndexManagementIndices.transformTargetMappings)
            // TODO: Read in the actual mappings from the source index and use that
            val response: CreateIndexResponse = client.admin().indices().suspendUntil { create(request, it) }
            if (!response.isAcknowledged) {
                logger.error("Failed to create the target index $index")
                throw TransformIndexException("Failed to create the target index")
            }
        }
    }

    @Suppress("ThrowsCount", "RethrowCaughtException")
    suspend fun index(docsToIndex: List<DocWriteRequest<*>>): Long {
        var updatableDocsToIndex = docsToIndex
        var indexTimeInMillis = 0L
        val nonRetryableFailures = mutableListOf<BulkItemResponse>()
        try {
            if (updatableDocsToIndex.isNotEmpty()) {
                val targetIndex = updatableDocsToIndex.first().index()
                logger.debug("Attempting to index ${updatableDocsToIndex.size} documents to $targetIndex")
                createTargetIndex(targetIndex)
                backoffPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                    val bulkRequest = BulkRequest().add(updatableDocsToIndex)
                    val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
                    indexTimeInMillis += bulkResponse.took.millis
                    val retryableFailures = mutableListOf<BulkItemResponse>()
                    (bulkResponse.items ?: arrayOf()).filter { it.isFailed }.forEach { failedResponse ->
                        if (failedResponse.status() == RestStatus.TOO_MANY_REQUESTS) {
                            retryableFailures.add(failedResponse)
                        } else {
                            nonRetryableFailures.add(failedResponse)
                        }
                    }
                    updatableDocsToIndex = retryableFailures.map { failure ->
                        updatableDocsToIndex[failure.itemId] as IndexRequest
                    }
                    if (updatableDocsToIndex.isNotEmpty()) {
                        throw ExceptionsHelper.convertToOpenSearchException(retryableFailures.first().failure.cause)
                    }
                }
            }
            if (nonRetryableFailures.isNotEmpty()) {
                logger.error("Failed to index ${nonRetryableFailures.size} documents")
                throw ExceptionsHelper.convertToOpenSearchException(nonRetryableFailures.first().failure.cause)
            }
            return indexTimeInMillis
        } catch (e: TransformIndexException) {
            throw e
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformIndexException("Failed to index the documents", unwrappedException)
        } catch (e: OpenSearchSecurityException) {
            throw TransformIndexException("Failed to index the documents - missing required index permissions: ${e.localizedMessage}", e)
        } catch (e: Exception) {
            throw TransformIndexException("Failed to index the documents", e)
        }
    }
}
