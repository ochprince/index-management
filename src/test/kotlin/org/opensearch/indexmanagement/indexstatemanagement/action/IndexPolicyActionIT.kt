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

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.junit.Assert
import com.colasoft.opensearch.client.ResponseException
import com.colasoft.opensearch.cluster.routing.allocation.AwarenessReplicaBalance
import com.colasoft.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.State
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.toJsonString
import com.colasoft.opensearch.indexmanagement.makeRequest
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class IndexPolicyActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test allocation aware replica count`() {
        val policyID = "${testIndexName}_testPolicyName_replica"
        var actionConfig = ReplicaCountAction(3, 0)
        var states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        updateClusterSetting(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.key, "true")
        updateClusterSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.key, "zone")

        // creates a dummy policy , so that ISM index gets initialized
        var policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        client().makeRequest(
            "PUT",
            "${IndexManagementPlugin.POLICY_BASE_URI}/init-index",
            emptyMap(),
            StringEntity(policy.toJsonString(), ContentType.APPLICATION_JSON)
        )

        updateClusterSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.key + "zone.values", "a, b")

        // Valid replica count, shouldn't throw exception
        client().makeRequest(
            "PUT",
            "${IndexManagementPlugin.POLICY_BASE_URI}/$policyID",
            emptyMap(),
            StringEntity(policy.toJsonString(), ContentType.APPLICATION_JSON)
        )

        actionConfig = ReplicaCountAction(4, 0)
        states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        Assert.assertThrows(
            ResponseException::class.java
        ) {
            client().makeRequest(
                "PUT",
                "${IndexManagementPlugin.POLICY_BASE_URI}/$policyID",
                emptyMap(),
                StringEntity(policy.toJsonString(), ContentType.APPLICATION_JSON)
            )
        }

        // clean up cluster settings
        updateClusterSetting(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.key, "true")
        updateClusterSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.key, "")
        updateClusterSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.key + "zone", "")
    }
}
