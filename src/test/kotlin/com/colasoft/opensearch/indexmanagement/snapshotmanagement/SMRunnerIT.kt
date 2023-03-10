/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement

import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import com.colasoft.opensearch.indexmanagement.makeRequest
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import com.colasoft.opensearch.indexmanagement.waitFor
import com.colasoft.opensearch.jobscheduler.spi.schedule.CronSchedule
import com.colasoft.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import com.colasoft.opensearch.rest.RestStatus
import java.time.Instant.now
import java.time.temporal.ChronoUnit

class SMRunnerIT : SnapshotManagementRestTestCase() {

    fun `test overall workflow`() {
        createRepository("repo")

        val smPolicy = randomSMPolicy(
            creationSchedule = CronSchedule("* * * * *", randomZone()),
            jobSchedule = IntervalSchedule(now(), 1, ChronoUnit.MINUTES),
            jobEnabled = true,
            jobEnabledTime = now(),
        )
        val policyName = smPolicy.policyName
        val response = client().makeRequest("POST", "$SM_POLICIES_URI/$policyName", emptyMap(), smPolicy.toHttpEntity())
        assertEquals("Create SM policy failed", RestStatus.CREATED, response.restStatus())

        // Initialization
        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNotNull(explainMetadata.creation!!.trigger.time)
        }

        // Wait for cron schedule to meet
        Thread.sleep(60_000L)

        // Create condition met
        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNotNull(explainMetadata.creation!!.started)
            assertEquals(SMMetadata.LatestExecution.Status.IN_PROGRESS, explainMetadata.creation.latestExecution!!.status)
        }

        // Snapshot has been created successfully
        updateSMPolicyStartTime(smPolicy)
        waitFor {
            val explainMetadata = parseExplainResponse(explainSMPolicy(policyName).entity.content).first()
            assertNull(explainMetadata.creation!!.started)
            assertEquals(SMMetadata.LatestExecution.Status.SUCCESS, explainMetadata.creation.latestExecution!!.status)
        }
    }
}
