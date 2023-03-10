/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.resthandler

import com.colasoft.opensearch.client.ResponseException
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.colasoft.opensearch.indexmanagement.makeRequest
import com.colasoft.opensearch.indexmanagement.rollup.RollupRestTestCase
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestDeleteRollupActionIT : RollupRestTestCase() {
    @Throws(Exception::class)
    fun `test deleting a rollup`() {
        val rollup = createRandomRollup()

        val deleteResponse = client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", "$ROLLUP_JOBS_BASE_URI/${rollup.id}")
        assertEquals("Deleted rollup still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting a rollup that doesn't exist in existing config index`() {
        try {
            createRandomRollup()
            client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test deleting a rollup that doesn't exist and config index doesnt exist`() {
        try {
            deleteIndex(INDEX_MANAGEMENT_INDEX)
            client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
