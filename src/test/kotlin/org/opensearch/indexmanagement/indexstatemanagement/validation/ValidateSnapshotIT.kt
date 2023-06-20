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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.validation

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.State
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import com.colasoft.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ValidateSnapshotIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test basic snapshot validation`() {
        val indexName = "${testIndexName}_index_basic"
        val policyID = "${testIndexName}_policy_basic"
        val repository = "repository"
        val snapshot = "snapshot"
        val actionConfig = SnapshotAction(repository, snapshot, 0)
        val states = listOf(
            State("Snapshot", listOf(actionConfig), listOf())
        )

        createRepository(repository)

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

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles for wait for snapshot step
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val data = getExplainValidationResult(indexName)
            assertEquals(
                "Index snapshot action validation status is PASSED.",
                Validate.ValidationStatus.PASSED,
                data.validationStatus
            )
        }

        waitFor { assertSnapshotExists(repository, "snapshot") }
        waitFor { assertSnapshotFinishedWithSuccess(repository, "snapshot") }
    }
}
