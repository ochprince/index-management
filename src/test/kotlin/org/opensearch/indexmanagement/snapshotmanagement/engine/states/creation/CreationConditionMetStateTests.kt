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

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import kotlinx.coroutines.runBlocking
import com.colasoft.opensearch.indexmanagement.MocksTestCase
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant.now

class CreationConditionMetStateTests : MocksTestCase() {

    fun `test next creation time met`() = runBlocking {
        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATION_START,
            nextCreationTime = now().minusSeconds(60),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotEquals("Next execution time should be updated.", metadata.creation.trigger.time, result.metadataToSave.build().creation.trigger.time)
    }

    fun `test next creation time has not met`() = runBlocking {
        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATION_START,
            nextCreationTime = now().plusSeconds(60),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertEquals("Next execution time should not be updated.", metadata, result.metadataToSave.build())
    }
}
