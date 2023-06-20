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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.colasoft.opensearch.client.ResponseException
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.State
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import com.colasoft.opensearch.indexmanagement.randomInstant
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.indexmanagement.waitFor
import com.colasoft.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ISMTemplateRestAPIIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    private val policyID1 = "t1"
    private val policyID2 = "t2"
    private val policyID3 = "t3"

    @Suppress("UNCHECKED_CAST")
    fun `test add template with invalid index pattern`() {
        try {
            val ismTemp = ISMTemplate(listOf(" "), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp)), policyID1)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "Validation Failed: 1: index_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test add template with self-overlapping index pattern`() {
        try {
            val ismTemp = ISMTemplate(listOf("ab*"), 100, randomInstant())
            val ismTemp2 = ISMTemplate(listOf("abc*"), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp, ismTemp2)), policyID1)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "New policy $policyID1 has an ISM template with index pattern [ab*] matching this policy's other ISM templates with index patterns [abc*], please use different priority"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test add template with overlapping index pattern`() {
        try {
            val ismTemp = ISMTemplate(listOf("log*"), 100, randomInstant())
            val ismTemp2 = ISMTemplate(listOf("abc*"), 100, randomInstant())
            val ismTemp3 = ISMTemplate(listOf("*"), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp)), policyID1)
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp2)), policyID2)
            createPolicy(randomPolicy(ismTemplate = listOf(ismTemp3)), policyID3)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "New policy $policyID3 has an ISM template with index pattern [*] matching existing policy templates, please use a different priority than 100"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    fun `test ism template managing index`() {
        disableValidationService()
        val indexName1 = "log-000001"
        val indexName2 = "log-000002"
        val indexName3 = "log-000003"
        val policyID = "${testIndexName}_testPolicyName_1"

        // need to specify policyID null, can remove after policyID deprecated
        createIndex(indexName1, null)

        val ismTemp = ISMTemplate(listOf("log*"), 100, randomInstant())

        val action = ReadOnlyAction(0)
        val states = listOf(
            State("ReadOnlyState", listOf(action), listOf())
        )
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = listOf(ismTemp)
        )
        createPolicy(policy, policyID)

        createIndex(indexName2, null)
        createIndex(indexName3, Settings.builder().put(INDEX_HIDDEN, true).build())

        waitFor { assertNotNull(getManagedIndexConfig(indexName2)) }

        // TODO uncomment in remove policy id
        // val managedIndexConfig = getExistingManagedIndexConfig(indexName2)
        // updateManagedIndexConfigStartTime(managedIndexConfig)
        // waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName2).policyID) }

        // only index create after template can be managed
        assertPredicatesOnMetaData(
            listOf(
                indexName1 to listOf(
                    explainResponseOpendistroPolicyIdSetting to fun(policyID: Any?): Boolean = policyID == null,
                    explainResponseOpenSearchPolicyIdSetting to fun(policyID: Any?): Boolean = policyID == null,
                    ManagedIndexMetaData.ENABLED to fun(enabled: Any?): Boolean = enabled == null
                )
            ),
            getExplainMap(indexName1),
            true
        )
        assertNull(getManagedIndexConfig(indexName1))

        // hidden index will not be manage
        assertPredicatesOnMetaData(
            listOf(
                indexName1 to listOf(
                    explainResponseOpendistroPolicyIdSetting to fun(policyID: Any?): Boolean = policyID == null,
                    explainResponseOpenSearchPolicyIdSetting to fun(policyID: Any?): Boolean = policyID == null,
                    ManagedIndexMetaData.ENABLED to fun(enabled: Any?): Boolean = enabled == null
                )
            ),
            getExplainMap(indexName1),
            true
        )
        assertNull(getManagedIndexConfig(indexName3))
    }
}
