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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction.Companion.MAX_NUM_SEGMENTS_FIELD
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ForceMergeActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val maxNumSegments = sin.readInt()
        val index = sin.readInt()
        return ForceMergeAction(maxNumSegments, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var maxNumSegments: Int? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                MAX_NUM_SEGMENTS_FIELD -> maxNumSegments = xcp.intValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ForceMergeActionConfig.")
            }
        }

        return ForceMergeAction(
            requireNotNull(maxNumSegments) { "ForceMergeActionConfig maxNumSegments is null" },
            index
        )
    }

    override fun getActionType(): String {
        return ForceMergeAction.name
    }
}
