/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.refreshanalyzer

import org.junit.AfterClass
import com.colasoft.opensearch.client.ResponseException
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.IndexManagementRestTestCase
import com.colasoft.opensearch.indexmanagement.makeRequest
import com.colasoft.opensearch.indexmanagement.refreshanalyzer.RestRefreshSearchAnalyzerAction.Companion.REFRESH_SEARCH_ANALYZER_BASE_URI
import com.colasoft.opensearch.rest.RestRequest.Method.POST
import com.colasoft.opensearch.rest.RestStatus

class RestRefreshSearchAnalyzerActionIT : IndexManagementRestTestCase() {

    companion object {
        @AfterClass
        @JvmStatic fun clearIndicesAfterClass() {
            wipeAllIndices()
        }
    }

    fun `test missing indices`() {
        try {
            client().makeRequest(POST.toString(), REFRESH_SEARCH_ANALYZER_BASE_URI)
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "illegal_argument_exception", "reason" to "Missing indices")
                    ),
                    "type" to "illegal_argument_exception",
                    "reason" to "Missing indices"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test closed index`() {
        val indexName = "testindex"
        val settings = Settings.builder().build()
        createIndex(indexName, settings)
        closeIndex(indexName)

        try {
            client().makeRequest(POST.toString(), "$REFRESH_SEARCH_ANALYZER_BASE_URI/$indexName")
            fail("Expected a failure")
        } catch (e: ResponseException) {
            val response = e.response.asMap()
            assertEquals(400, response.get("status"))
            assertEquals("index_closed_exception", (response.get("error") as HashMap<*, *>).get("type"))
        }
    }
}
