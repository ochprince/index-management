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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.common.logging.DeprecationLogger
import com.colasoft.opensearch.common.xcontent.XContentHelper
import com.colasoft.opensearch.core.xcontent.MediaType
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.parseClusterManagerTimeout
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.POST
import com.colasoft.opensearch.rest.action.RestToXContentListener

class RestRetryFailedManagedIndexAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, RETRY_BASE_URI,
                POST, LEGACY_RETRY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$RETRY_BASE_URI/{index}",
                POST, "$LEGACY_RETRY_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String {
        return "retry_failed_managed_index"
    }

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }
        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType as (MediaType)).v2()
        } else {
            mapOf()
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val clusterManagerTimeout = parseClusterManagerTimeout(
            request, DeprecationLogger.getLogger(RestRetryFailedManagedIndexAction::class.java), name
        )

        val retryFailedRequest = RetryFailedManagedIndexRequest(
            indices.toList(), body["state"] as String?,
            clusterManagerTimeout,
            indexType
        )

        return RestChannelConsumer { channel ->
            client.execute(RetryFailedManagedIndexAction.INSTANCE, retryFailedRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val RETRY_BASE_URI = "$ISM_BASE_URI/retry"
        const val LEGACY_RETRY_BASE_URI = "$LEGACY_ISM_BASE_URI/retry"
    }
}
