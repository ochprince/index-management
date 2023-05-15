/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.model

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.core.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class StateFilter(val state: String) : Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        state = sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(state)
    }

    companion object {
        const val STATE_FIELD = "state"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): StateFilter {
            var state: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    STATE_FIELD -> state = xcp.text()
                }
            }

            return StateFilter(requireNotNull(state) { "Must include a state when using include filter" })
        }
    }
}
