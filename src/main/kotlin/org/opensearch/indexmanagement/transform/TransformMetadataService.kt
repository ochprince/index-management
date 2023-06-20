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

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.ExceptionsHelper
import com.colasoft.opensearch.action.DocWriteRequest
import com.colasoft.opensearch.action.DocWriteResponse
import com.colasoft.opensearch.action.get.GetRequest
import com.colasoft.opensearch.action.get.GetResponse
import com.colasoft.opensearch.action.index.IndexRequest
import com.colasoft.opensearch.action.index.IndexResponse
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.common.xcontent.LoggingDeprecationHandler
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.common.xcontent.XContentFactory
import com.colasoft.opensearch.common.xcontent.XContentHelper
import com.colasoft.opensearch.common.xcontent.XContentType
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.opensearchapi.suspendUntil
import com.colasoft.opensearch.indexmanagement.transform.exceptions.TransformMetadataException
import com.colasoft.opensearch.indexmanagement.transform.model.ContinuousTransformStats
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.indexmanagement.transform.model.TransformMetadata
import com.colasoft.opensearch.indexmanagement.transform.model.TransformStats
import com.colasoft.opensearch.indexmanagement.util.IndexUtils.Companion.hashToFixedSize
import com.colasoft.opensearch.transport.RemoteTransportException
import java.time.Instant

@SuppressWarnings("ReturnCount")
class TransformMetadataService(private val client: Client, val xContentRegistry: NamedXContentRegistry) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("BlockingMethodInNonBlockingContext")
    suspend fun getMetadata(transform: Transform): TransformMetadata {
        return if (transform.metadataId != null) {
            // update metadata
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, transform.metadataId).routing(transform.id)
            val response: GetResponse = client.suspendUntil { get(getRequest, it) }
            val metadataSource = response.sourceAsBytesRef
            val transformMetadata = metadataSource?.let {
                withContext(Dispatchers.IO) {
                    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                    xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, TransformMetadata.Companion::parse)
                }
            }
            // TODO: Should we attempt to create a new document instead if failed to parse, the only reason this can happen is if someone deleted
            //  the metadata doc?
            transformMetadata ?: throw TransformMetadataException("Failed to parse the existing metadata document")
        } else {
            logger.debug("Creating metadata doc as none exists at the moment for transform job [${transform.id}]")
            createMetadata(transform)
        }
    }

    private suspend fun createMetadata(transform: Transform): TransformMetadata {
        // Including timestamp in the metadata id to prevent clashes if the job was deleted but metadata is not deleted, in that case we want to
        // create a clean metadata doc
        val id = hashToFixedSize("TransformMetadata#${transform.id}#${transform.lastUpdateTime}")
        val metadata = TransformMetadata(
            id = id,
            transformId = transform.id,
            lastUpdatedAt = Instant.now(),
            status = TransformMetadata.Status.INIT,
            stats = TransformStats(0, 0, 0, 0, 0),
            continuousStats = if (transform.continuous) ContinuousTransformStats(null, null) else null
        )
        return writeMetadata(metadata)
    }

    @Suppress("BlockingMethodInNonBlockingContext", "ThrowsCount", "ComplexMethod")
    suspend fun writeMetadata(metadata: TransformMetadata, updating: Boolean = false): TransformMetadata {
        val errorMessage = "Failed to ${if (updating) "update" else "create"} metadata doc ${metadata.id} for transform job ${metadata.transformId}"
        try {
            val builder = metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
                .source(builder)
                .id(metadata.id)
                .routing(metadata.transformId)
            if (updating) {
                indexRequest.setIfSeqNo(metadata.seqNo).setIfPrimaryTerm(metadata.primaryTerm)
            } else {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            }

            val response: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            return when (response.result) {
                DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED -> {
                    metadata.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm)
                }
                else -> {
                    logger.error(errorMessage)
                    throw TransformMetadataException("Failed to write metadata, received ${response.result?.lowercase} status")
                }
            }
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error(errorMessage, unwrappedException)
            throw TransformMetadataException(errorMessage, unwrappedException)
        } catch (e: Exception) {
            logger.error(errorMessage, e)
            throw TransformMetadataException(errorMessage, e)
        }
    }
}
