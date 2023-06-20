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

package com.colasoft.opensearch.indexmanagement.refreshanalyzer

import org.junit.Assert
import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.index.Index
import com.colasoft.opensearch.index.shard.ShardId
import com.colasoft.opensearch.test.OpenSearchTestCase

class RefreshSearchAnalyzerShardResponseTests : OpenSearchTestCase() {

    fun `test shard refresh response parsing`() {
        val reloadedAnalyzers = listOf("analyzer1", "analyzer2")
        val refreshShardResponse = RefreshSearchAnalyzerShardResponse(ShardId(Index("testIndex", "qwerty"), 0), reloadedAnalyzers)

        val refreshShardResponse2 = roundTripRequest(refreshShardResponse)
        Assert.assertEquals(refreshShardResponse2.shardId, refreshShardResponse.shardId)
    }

    @Throws(Exception::class)
    private fun roundTripRequest(response: RefreshSearchAnalyzerShardResponse): RefreshSearchAnalyzerShardResponse {
        BytesStreamOutput().use { out ->
            response.writeTo(out)
            out.bytes().streamInput().use { si -> return RefreshSearchAnalyzerShardResponse(si) }
        }
    }
}
