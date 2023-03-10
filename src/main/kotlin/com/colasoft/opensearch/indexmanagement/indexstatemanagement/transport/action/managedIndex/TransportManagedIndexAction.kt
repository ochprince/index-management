/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex

import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.support.ActionFilters
import com.colasoft.opensearch.action.support.HandledTransportAction
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.tasks.Task
import com.colasoft.opensearch.transport.TransportService

/**
 * This is a non operational transport action that is used by ISM to check if the user has required index permissions to manage index
 */
class TransportManagedIndexAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
) : HandledTransportAction<ManagedIndexRequest, AcknowledgedResponse>(
    ManagedIndexAction.NAME, transportService, actionFilters, ::ManagedIndexRequest
) {

    override fun doExecute(task: Task, request: ManagedIndexRequest, listener: ActionListener<AcknowledgedResponse>) {
        // Do nothing
        return listener.onResponse(AcknowledgedResponse(true))
    }
}
