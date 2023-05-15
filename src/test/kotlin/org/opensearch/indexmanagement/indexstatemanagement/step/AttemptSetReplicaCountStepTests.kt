/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.mockito.Mockito.doAnswer
import com.colasoft.opensearch.action.ActionListener
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.client.AdminClient
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.client.IndicesAdminClient
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.ReplicaCountAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.replicacount.AttemptReplicaCountStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import com.colasoft.opensearch.jobscheduler.spi.utils.LockService
import com.colasoft.opensearch.script.ScriptService
import com.colasoft.opensearch.test.OpenSearchTestCase
import com.colasoft.opensearch.transport.RemoteTransportException

class AttemptSetReplicaCountStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)

    fun `test replica step sets step status to failed when not acknowledged`() {
        val replicaCountResponse = AcknowledgedResponse(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(replicaCountResponse, null)))

        runBlocking {
            val replicaCountAction = ReplicaCountAction(2, 0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val replicaCountStep = AttemptReplicaCountStep(replicaCountAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            replicaCountStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = replicaCountStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test replica step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val replicaCountAction = ReplicaCountAction(2, 0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val replicaCountStep = AttemptReplicaCountStep(replicaCountAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            replicaCountStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = replicaCountStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test replica step sets step status to failed when remote transport error thrown`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val replicaCountAction = ReplicaCountAction(2, 0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val replicaCountStep = AttemptReplicaCountStep(replicaCountAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            replicaCountStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = replicaCountStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(replicaResponse: AcknowledgedResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (replicaResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                if (replicaResponse != null) listener.onResponse(replicaResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).updateSettings(any(), any())
        }
    }
}
