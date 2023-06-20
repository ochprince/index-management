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

import com.colasoft.opensearch.action.support.broadcast.BroadcastShardResponse
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.index.shard.ShardId
import java.io.IOException

class RefreshSearchAnalyzerShardResponse : BroadcastShardResponse {
    var reloadedAnalyzers: List<String>

    constructor(si: StreamInput) : super(si) {
        reloadedAnalyzers = si.readStringArray().toList()
    }

    constructor(shardId: ShardId, reloadedAnalyzers: List<String>) : super(shardId) {
        this.reloadedAnalyzers = reloadedAnalyzers
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeStringArray(reloadedAnalyzers.toTypedArray())
    }
}
