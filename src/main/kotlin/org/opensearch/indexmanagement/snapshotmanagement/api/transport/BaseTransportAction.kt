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

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.ActionRequest
import com.colasoft.opensearch.action.ActionResponse
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.common.util.concurrent.ThreadContext.StoredContext
import com.colasoft.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.index.engine.VersionConflictEngineException
import com.colasoft.opensearch.indexmanagement.util.IndexManagementException
import com.colasoft.opensearch.indexmanagement.util.SecurityUtils
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService

abstract class BaseTransportAction<Request : ActionRequest, Response : ActionResponse>(
    name: String,
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    requestReader: Writeable.Reader<Request>,
) : HandledTransportAction<Request, Response>(
    name, transportService, actionFilters, requestReader
) {

    private val log = LogManager.getLogger(javaClass)
    private val coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.IO)

    override fun doExecute(
        task: Task,
        request: Request,
        listener: ActionListener<Response>
    ) {
        log.debug(
            "user and roles string from thread context: " +
                client.threadPool().threadContext.getTransient<String>(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        )
        val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext)
        coroutineScope.launch {
            try {
                client.threadPool().threadContext.stashContext().use { threadContext ->
                    listener.onResponse(executeRequest(request, user, threadContext))
                }
            } catch (ex: IndexManagementException) {
                listener.onFailure(ex)
            } catch (ex: VersionConflictEngineException) {
                listener.onFailure(ex)
            } catch (ex: OpenSearchStatusException) {
                listener.onFailure(ex)
            } catch (ex: Exception) {
                log.error("Uncaught exception:", ex)
                listener.onFailure(OpenSearchStatusException(ex.message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }
    }

    abstract suspend fun executeRequest(request: Request, user: User?, threadContext: StoredContext): Response
}
