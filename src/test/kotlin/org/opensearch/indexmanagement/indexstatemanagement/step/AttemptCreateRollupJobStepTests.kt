/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.step

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomRollupActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.test.OpenSearchTestCase

class AttemptCreateRollupJobStepTests : OpenSearchTestCase() {

    private val rollupAction = randomRollupActionConfig()
    private val indexName = "test"
    private val rollupId: String = rollupAction.ismRollup.toRollup(indexName).id
    private val metadata = ManagedIndexMetaData(
        indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
        ActionMetaData(AttemptCreateRollupJobStep.name, 1, 0, false, 0, null, ActionProperties(rollupId = rollupId)), null, null, null
    )
    private val step = AttemptCreateRollupJobStep(rollupAction)

    fun `test process failure`() {
        step.processFailure(rollupId, indexName, Exception("dummy-error"))
        val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        assertEquals(
            "Error message is not expected",
            AttemptCreateRollupJobStep.getFailedMessage(rollupId, indexName),
            updatedManagedIndexMetaData.info?.get("message")
        )
    }

    fun `test isIdempotent`() {
        assertTrue(step.isIdempotent())
    }
}
