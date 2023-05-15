/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.State
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.destination.CustomWebhook
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.destination.Destination
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.destination.DestinationType
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.colasoft.opensearch.indexmanagement.makeRequest
import com.colasoft.opensearch.indexmanagement.waitFor
import com.colasoft.opensearch.script.Script
import com.colasoft.opensearch.script.ScriptType
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class NotificationActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    // TODO: this seems to have broken with the notification plugin
    // cannot test chime/slack in integ tests, but can test a custom webhook by
    // using the POST call to write to the local integTest cluster and verify that index has 1 doc
    @Suppress("UNCHECKED_CAST")
    fun `skip test custom webhook notification`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_testPolicyName"
        val notificationIndex = "notification_index"
        val clusterUri = System.getProperty("tests.rest.cluster").split(",")[0]
        val destination = Destination(
            type = DestinationType.CUSTOM_WEBHOOK,
            chime = null,
            slack = null,
            customWebhook = CustomWebhook(
                url = "$protocol://$clusterUri/$notificationIndex/_doc",
                scheme = null,
                host = null,
                port = -1,
                path = null,
                queryParams = emptyMap(),
                headerParams = mapOf("Content-Type" to "application/json"),
                username = if (securityEnabled()) "admin" else null,
                password = if (securityEnabled()) "admin" else null
            )
        )
        val messageTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{ \"testing\": 5 }", emptyMap())
        val actionConfig = NotificationAction(destination = destination, channel = null, messageTemplate = messageTemplate, index = 0)
        val states = listOf(State(name = "NotificationState", actions = listOf(actionConfig), transitions = emptyList()))
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
        createIndex(notificationIndex, null)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // verify index does not have any docs
        assertEquals(
            "Notification index has docs before notification has been sent", 0,
            (
                client().makeRequest("GET", "$notificationIndex/_search")
                    .asMap() as Map<String, Map<String, Map<String, Any>>>
                )["hits"]!!["total"]!!["value"]
        )

        // Speed up to second execution where it will trigger the first execution of the action which
        // should call notification custom webhook and create the doc in notification_index
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // verify index gets a doc
        waitFor {
            assertEquals(
                "Notification index does not have a doc", 1,
                (
                    client().makeRequest("GET", "$notificationIndex/_search")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"]
            )
        }
    }
}
