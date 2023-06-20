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

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.resthandler

import com.colasoft.opensearch.client.ResponseException
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.makeRequest
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import com.colasoft.opensearch.rest.RestStatus
import java.time.Instant

class RestStartSnapshotManagementIT : SnapshotManagementRestTestCase() {

    fun `test starting a stopped snapshot management policy`() {
        val smPolicy = createSMPolicy(randomSMPolicy().copy(jobEnabled = false, jobEnabledTime = null))
        assertFalse("Snapshot management policy was not disabled", smPolicy.jobEnabled)

        val response = client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}/${smPolicy.policyName}/_start")
        assertEquals("Start snapshot management policy failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedSMPolicy = getSMPolicy(smPolicy.policyName)
        assertTrue("Snapshot management policy was not enabled", updatedSMPolicy.jobEnabled)
    }

    fun `test starting an enabled snapshot management policy`() {
        val smPolicy = createSMPolicy(randomSMPolicy().copy(jobEnabled = true, jobEnabledTime = Instant.now()))
        assertTrue("Snapshot management policy should be enabled", smPolicy.jobEnabled)

        val response = client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}/${smPolicy.policyName}/_start")
        assertEquals("Start snapshot management policy failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedSMPolicy = getSMPolicy(smPolicy.policyName)
        assertTrue("Snapshot management policy was not enabled", updatedSMPolicy.jobEnabled)
    }

    fun `test starting a snapshot management policy with an invalid id fails`() {
        // Test with no ID
        try {
            client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}//_start")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
        // Test with a nonexistent ID
        try {
            client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}/${randomAlphaOfLength(20).lowercase()}/_start")
            fail("Expected NOT_FOUND response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test starting a snapshot management policy with no config index fails`() {
        try {
            deleteIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
            client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}/nonexistent_foo/_start")
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
