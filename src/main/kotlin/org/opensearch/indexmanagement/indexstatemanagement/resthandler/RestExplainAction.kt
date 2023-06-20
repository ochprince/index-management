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

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.common.logging.DeprecationLogger
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainRequest
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_EXPLAIN_VALIDATE_ACTION
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_EXPLAIN_SHOW_POLICY
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.SHOW_VALIDATE_ACTION
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_JOB_SORT_FIELD
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.SHOW_POLICY_QUERY_PARAM
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.parseClusterManagerTimeout
import com.colasoft.opensearch.indexmanagement.util.getSearchParams
import com.colasoft.opensearch.rest.BaseRestHandler
import com.colasoft.opensearch.rest.BaseRestHandler.RestChannelConsumer
import com.colasoft.opensearch.rest.RestHandler.ReplacedRoute
import com.colasoft.opensearch.rest.RestHandler.Route
import com.colasoft.opensearch.rest.RestRequest
import com.colasoft.opensearch.rest.RestRequest.Method.GET
import com.colasoft.opensearch.rest.action.RestToXContentListener

private val log = LogManager.getLogger(RestExplainAction::class.java)

class RestExplainAction : BaseRestHandler() {

    companion object {
        const val EXPLAIN_BASE_URI = "$ISM_BASE_URI/explain"
        const val LEGACY_EXPLAIN_BASE_URI = "$LEGACY_ISM_BASE_URI/explain"
    }

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, EXPLAIN_BASE_URI,
                GET, LEGACY_EXPLAIN_BASE_URI
            ),
            ReplacedRoute(
                GET, "$EXPLAIN_BASE_URI/{index}",
                GET, "$LEGACY_EXPLAIN_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String {
        return "ism_explain_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val indices: Array<String> = Strings.splitStringByCommaToArray(request.param("index"))

        val searchParams = request.getSearchParams(DEFAULT_JOB_SORT_FIELD)

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val clusterManagerTimeout = parseClusterManagerTimeout(
            request, DeprecationLogger.getLogger(RestExplainAction::class.java), name
        )

        val explainRequest = ExplainRequest(
            indices.toList(),
            request.paramAsBoolean("local", false),
            clusterManagerTimeout,
            searchParams,
            request.paramAsBoolean(SHOW_POLICY_QUERY_PARAM, DEFAULT_EXPLAIN_SHOW_POLICY),
            request.paramAsBoolean(SHOW_VALIDATE_ACTION, DEFAULT_EXPLAIN_VALIDATE_ACTION),
            indexType
        )

        return RestChannelConsumer { channel ->
            client.execute(ExplainAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
