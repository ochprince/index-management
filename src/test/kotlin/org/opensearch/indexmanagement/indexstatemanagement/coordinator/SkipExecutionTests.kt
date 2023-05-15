/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.coordinator

import org.junit.Before
import org.mockito.Mockito
import com.colasoft.opensearch.action.admin.cluster.node.info.NodesInfoAction
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.ClusterChangedEvent
import com.colasoft.opensearch.cluster.OpenSearchAllocationTestCase
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.SkipExecution

class SkipExecutionTests : OpenSearchAllocationTestCase() {

    private lateinit var client: Client
    private lateinit var skip: SkipExecution

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        skip = SkipExecution(client)
    }

    fun `test cluster change event`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        Mockito.`when`(event.nodesChanged()).thenReturn(true)
        skip.sweepISMPluginVersion()
        Mockito.verify(client).execute(Mockito.eq(NodesInfoAction.INSTANCE), Mockito.any(), Mockito.any())
    }
}
