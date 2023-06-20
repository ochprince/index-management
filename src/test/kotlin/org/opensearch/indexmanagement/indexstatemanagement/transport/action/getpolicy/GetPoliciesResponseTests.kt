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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.common.xcontent.XContentFactory
import com.colasoft.opensearch.common.xcontent.XContentHelper
import com.colasoft.opensearch.common.xcontent.json.JsonXContent
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.extension.SampleCustomActionParser
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.State
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import com.colasoft.opensearch.indexmanagement.opensearchapi.convertToMap
import com.colasoft.opensearch.indexmanagement.opensearchapi.string
import com.colasoft.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

class GetPoliciesResponseTests : OpenSearchTestCase() {

    fun `test get policies response`() {
        val policy = randomPolicy()
        val res = GetPoliciesResponse(listOf(policy), 1)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetPoliciesResponse(sin)
        assertEquals(1, newRes.totalPolicies)
        assertEquals(1, newRes.policies.size)
        assertEquals(policy, newRes.policies[0])
    }

    @Suppress("UNCHECKED_CAST")
    fun `test get policies response custom action`() {
        val customActionParser = SampleCustomActionParser()
        customActionParser.customAction = true
        val extensionName = "testExtension"
        ISMActionsParser.instance.addParser(customActionParser, extensionName)
        val policyID = "policyID"
        val action = SampleCustomActionParser.SampleCustomAction(someInt = randomInt(), index = 0)
        val states = listOf(State(name = "CustomState", actions = listOf(action), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        val res = GetPoliciesResponse(listOf(policy), 1)

        val responseString = res.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
        val responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, responseString, false)
        assertEquals("Round tripping custom action doesn't work", res.convertToMap(), responseMap)
        assertNotEquals("Get policies response should change the policy output", responseMap, policy.convertToMap())
        val parsedPolicy = (responseMap["policies"] as ArrayList<Map<String, Any>>).first()["policy"] as Map<String, Any>
        val parsedStates = parsedPolicy["states"] as ArrayList<Map<String, Any>>
        val parsedActions = parsedStates.first()["actions"] as ArrayList<Map<String, Any>>
        assertFalse("Get policies response should not contain the custom keyword", parsedActions.first().containsKey("custom"))
        ISMActionsParser.instance.parsers.removeIf { it.getActionType() == SampleCustomActionParser.SampleCustomAction.name }
    }
}
