/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.tryUpdatingNextExecutionTime

object CreationConditionMetState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.CREATION)

        val nextCreationTime = metadata.creation.trigger.time
        val updateNextTimeResult = tryUpdatingNextExecutionTime(
            metadataBuilder, nextCreationTime, job.creation.schedule, WorkflowType.CREATION, log
        )
        if (!updateNextTimeResult.updated) {
            return SMResult.Stay(metadataBuilder)
        }
        metadataBuilder = updateNextTimeResult.metadataBuilder

        return SMResult.Next(metadataBuilder)
    }
}
