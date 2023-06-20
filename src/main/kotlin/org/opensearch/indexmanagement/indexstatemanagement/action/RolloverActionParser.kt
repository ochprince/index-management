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
import com.colasoft.opensearch.common.unit.ByteSizeValue
import com.colasoft.opensearch.common.unit.TimeValue
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class RolloverActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val minSize = sin.readOptionalWriteable(::ByteSizeValue)
        val minDocs = sin.readOptionalLong()
        val minAge = sin.readOptionalTimeValue()
        val minPrimaryShardSize = sin.readOptionalWriteable(::ByteSizeValue)
        val index = sin.readInt()

        return RolloverAction(minSize, minDocs, minAge, minPrimaryShardSize, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var minSize: ByteSizeValue? = null
        var minDocs: Long? = null
        var minAge: TimeValue? = null
        var minPrimaryShardSize: ByteSizeValue? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                RolloverAction.MIN_SIZE_FIELD -> minSize = ByteSizeValue.parseBytesSizeValue(xcp.text(), RolloverAction.MIN_SIZE_FIELD)
                RolloverAction.MIN_DOC_COUNT_FIELD -> minDocs = xcp.longValue()
                RolloverAction.MIN_INDEX_AGE_FIELD -> minAge = TimeValue.parseTimeValue(xcp.text(), RolloverAction.MIN_INDEX_AGE_FIELD)
                RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD -> minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue(
                    xcp.text(),
                    RolloverAction
                        .MIN_PRIMARY_SHARD_SIZE_FIELD
                )
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RolloverAction.")
            }
        }

        return RolloverAction(minSize, minDocs, minAge, minPrimaryShardSize, index)
    }

    override fun getActionType(): String {
        return RolloverAction.name
    }
}
