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

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.tryUpdatingNextExecutionTime
import java.time.Instant.now

// check the status of creating, deleting snapshot
object DeletionConditionMetState : State {

    override val continuous = true

    @Suppress("ReturnCount")
    override suspend fun execute(context: SMStateMachine): SMResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)

        if (job.deletion == null) {
            log.warn("Policy deletion config becomes null before checking if delete schedule met. Reset.")
            return SMResult.Fail(
                metadataBuilder.resetDeletion(), WorkflowType.DELETION, forceReset = true
            )
        }

        // if job.deletion != null, then metadata.deletion.trigger.time should already be
        //  initialized or handled in handlePolicyChange before executing this state.
        val nextDeletionTime = if (metadata.deletion == null) {
            val nextTime = job.deletion.schedule.getNextExecutionTime(now())
            metadataBuilder.setNextDeletionTime(nextTime)
            nextTime
        } else {
            metadata.deletion.trigger.time
        }
        val updateNextTimeResult = tryUpdatingNextExecutionTime(
            metadataBuilder, nextDeletionTime, job.deletion.schedule, WorkflowType.DELETION, log
        )
        if (!updateNextTimeResult.updated) {
            return SMResult.Stay(metadataBuilder)
        }
        metadataBuilder = updateNextTimeResult.metadataBuilder

        return SMResult.Next(metadataBuilder)
    }
}
