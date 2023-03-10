/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParserUtils
import com.colasoft.opensearch.indexmanagement.rollup.model.ISMRollup
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class RollupActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val ismRollup = ISMRollup(sin)
        val index = sin.readInt()
        return RollupAction(ismRollup, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var ismRollup: ISMRollup? = null

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                RollupAction.ISM_ROLLUP_FIELD -> ismRollup = ISMRollup.parse(xcp)
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RollupAction.")
            }
        }

        return RollupAction(ismRollup = requireNotNull(ismRollup) { "RollupAction rollup is null" }, index)
    }

    override fun getActionType(): String {
        return RollupAction.name
    }
}
