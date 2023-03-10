/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata

object CreationStartState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val metadataToSave = SMMetadata.Builder(context.metadata)
            .workflow(WorkflowType.CREATION)
            .setCurrentState(SMState.CREATION_START)

        return SMResult.Next(metadataToSave)
    }
}
