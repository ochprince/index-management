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
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ReplicaCountActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val numOfReplicas = sin.readInt()
        val index = sin.readInt()
        return ReplicaCountAction(numOfReplicas, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var numOfReplicas: Int? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                ReplicaCountAction.NUMBER_OF_REPLICAS_FIELD -> numOfReplicas = xcp.intValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ReplicaCountActionConfig.")
            }
        }

        return ReplicaCountAction(
            numOfReplicas = requireNotNull(numOfReplicas) { "$ReplicaCountAction.NUMBER_OF_REPLICAS_FIELD is null" },
            index = index
        )
    }

    override fun getActionType(): String {
        return ReplicaCountAction.name
    }
}
