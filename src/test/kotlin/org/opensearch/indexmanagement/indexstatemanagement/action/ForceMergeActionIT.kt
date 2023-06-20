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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.cluster.metadata.IndexMetadata
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.State
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.AttemptCallForceMergeStep
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.AttemptSetReadOnlyStep
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.WaitForForceMergeStep
import com.colasoft.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ForceMergeActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test basic workflow`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"

        // Create a Policy with one State that only preforms a force_merge Action
        val forceMergeActionConfig = ForceMergeAction(maxNumSegments = 1, index = 0)
        val states = listOf(State("ForceMergeState", listOf(forceMergeActionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        // Add sample data to increase segment count, passing in a delay to ensure multiple segments get created
        insertSampleData(indexName, 3, 1000)

        waitFor { assertTrue("Segment count for [$indexName] was less than expected", validateSegmentCount(indexName, min = 2)) }

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second execution: Index is set to read-only for force_merge
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }

        // Third execution: Force merge operation is kicked off
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // verify we set maxNumSegments in action properties when kicking off force merge
        waitFor {
            assertEquals(
                "maxNumSegments not set in ActionProperties",
                forceMergeActionConfig.maxNumSegments,
                getExplainManagedIndexMetaData(indexName).actionMetaData?.actionProperties?.maxNumSegments
            )
        }

        // Fourth execution: Waits for force merge to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertTrue("Segment count for [$indexName] after force merge is incorrect", validateSegmentCount(indexName, min = 1, max = 1)) }
        // verify we reset actionproperties at end of forcemerge
        waitFor { assertNull("maxNumSegments was not reset", getExplainManagedIndexMetaData(indexName).actionMetaData?.actionProperties) }
        // index should still be readonly after force merge finishes
        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }
    }

    fun `test force merge on index already in read-only`() {
        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"

        // Create a Policy with one State that only preforms a force_merge Action
        val forceMergeActionConfig = ForceMergeAction(maxNumSegments = 1, index = 0)
        val states = listOf(State("ForceMergeState", listOf(forceMergeActionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        // Add sample data to increase segment count, passing in a delay to ensure multiple segments get created
        insertSampleData(indexName, 3, 1000)

        waitFor { assertTrue("Segment count for [$indexName] was less than expected", validateSegmentCount(indexName, min = 2)) }

        // Set index to read-only
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true))

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second execution: Index was already read-only and should remain so for force_merge
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(AttemptSetReadOnlyStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }

        // Third execution: Force merge operation is kicked off
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(AttemptCallForceMergeStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // Fourth execution: Waits for force merge to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(WaitForForceMergeStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }
        assertTrue("Segment count for [$indexName] after force merge is incorrect", validateSegmentCount(indexName, min = 1, max = 1))
        assertEquals("true", getIndexBlocksWriteSetting(indexName))
    }
}
