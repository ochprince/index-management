/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction.Companion.REPOSITORY_FIELD
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction.Companion.SNAPSHOT_FIELD
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class SnapshotActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val repository = sin.readString()
        val snapshot = sin.readString()
        val index = sin.readInt()

        return SnapshotAction(repository, snapshot, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var repository: String? = null
        var snapshot: String? = null

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                REPOSITORY_FIELD -> repository = xcp.text()
                SNAPSHOT_FIELD -> snapshot = xcp.text()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in SnapshotAction.")
            }
        }

        return SnapshotAction(
            repository = requireNotNull(repository) { "SnapshotAction repository must be specified" },
            snapshot = requireNotNull(snapshot) { "SnapshotAction snapshot must be specified" },
            index = index
        )
    }

    override fun getActionType(): String {
        return SnapshotAction.name
    }
}
