/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import kotlinx.coroutines.runBlocking
import com.colasoft.opensearch.indexmanagement.MocksTestCase
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy

class DeletionStartStateTests : MocksTestCase() {

    fun `test start state execution`() = runBlocking {
        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_FINISHED
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETION_START.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
    }
}
