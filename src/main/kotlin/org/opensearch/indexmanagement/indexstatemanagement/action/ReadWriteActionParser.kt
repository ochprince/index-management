/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ReadWriteActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val index = sin.readInt()
        return ReadWriteAction(index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        ensureExpectedToken(XContentParser.Token.END_OBJECT, xcp.nextToken(), xcp)

        return ReadWriteAction(index)
    }

    override fun getActionType(): String {
        return ReadWriteAction.name
    }
}
