/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.refreshanalyzer

import com.colasoft.opensearch.action.support.broadcast.BroadcastRequest
import com.colasoft.opensearch.common.io.stream.StreamInput
import java.io.IOException

class RefreshSearchAnalyzerRequest : BroadcastRequest<RefreshSearchAnalyzerRequest> {
    @Suppress("SpreadOperator")
    constructor(vararg indices: String) : super(*indices)

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp)
}
