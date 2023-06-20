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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus
import com.colasoft.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse
import com.colasoft.opensearch.client.AdminClient
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.client.ClusterAdminClient
import com.colasoft.opensearch.cluster.SnapshotsInProgress
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import com.colasoft.opensearch.jobscheduler.spi.utils.LockService
import com.colasoft.opensearch.script.ScriptService
import com.colasoft.opensearch.snapshots.Snapshot
import com.colasoft.opensearch.snapshots.SnapshotId
import com.colasoft.opensearch.test.OpenSearchTestCase
import com.colasoft.opensearch.transport.RemoteTransportException

class WaitForSnapshotStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)
    val snapshot = "snapshot-name"

    fun `test snapshot missing snapshot name in action properties`() {
        val exception = IllegalArgumentException("not used")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val emptyActionProperties = ActionProperties()
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, emptyActionProperties), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", WaitForSnapshotStep.getFailedActionPropertiesMessage("test", emptyActionProperties), updatedManagedIndexMetaData.info!!["message"])
        }

        runBlocking {
            val nullActionProperties = null
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, nullActionProperties), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", WaitForSnapshotStep.getFailedActionPropertiesMessage("test", nullActionProperties), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot status states`() {
        val snapshotStatus: SnapshotStatus = mock()
        val response: SnapshotsStatusResponse = mock()
        whenever(response.snapshots).doReturn(listOf(snapshotStatus))
        whenever(snapshotStatus.snapshot).doReturn(Snapshot("repo", SnapshotId("snapshot-name", "some_uuid")))
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.INIT)
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot in progress message", WaitForSnapshotStep.getSnapshotInProgressMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.STARTED)
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot in progress message", WaitForSnapshotStep.getSnapshotInProgressMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.SUCCESS)
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot completed message", WaitForSnapshotStep.getSuccessMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.ABORTED)
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.FAILED)
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot not in response list`() {
        val snapshotStatus: SnapshotStatus = mock()
        val response: SnapshotsStatusResponse = mock()
        whenever(response.snapshots).doReturn(listOf(snapshotStatus))
        whenever(snapshotStatus.snapshot).doReturn(Snapshot("repo", SnapshotId("snapshot-different-name", "some_uuid")))
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))

        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot exception`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "example", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test snapshot remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }
    private fun getClusterAdminClient(snapshotsStatusResponse: SnapshotsStatusResponse?, exception: Exception?): ClusterAdminClient {
        assertTrue("Must provide one and only one response or exception", (snapshotsStatusResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<SnapshotsStatusResponse>>(1)
                if (snapshotsStatusResponse != null) listener.onResponse(snapshotsStatusResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).snapshotsStatus(any(), any())
        }
    }
}
