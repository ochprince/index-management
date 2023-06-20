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

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.common.io.stream.Writeable

class RefreshSearchAnalyzerAction : ActionType<RefreshSearchAnalyzerResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/refresh_search_analyzers"
        val INSTANCE = RefreshSearchAnalyzerAction()
        val reader = Writeable.Reader { inp -> RefreshSearchAnalyzerResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<RefreshSearchAnalyzerResponse> = reader
}
