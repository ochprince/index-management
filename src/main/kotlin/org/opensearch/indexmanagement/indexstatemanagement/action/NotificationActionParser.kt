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
import com.colasoft.opensearch.core.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction.Companion.CHANNEL_FIELD
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction.Companion.DESTINATION_FIELD
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction.Companion.MESSAGE_TEMPLATE_FIELD
import com.colasoft.opensearch.indexmanagement.common.model.notification.Channel
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.destination.Destination
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import com.colasoft.opensearch.script.Script

class NotificationActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val destination = sin.readOptionalWriteable(::Destination)
        val channel = sin.readOptionalWriteable(::Channel)
        val messageTemplate = Script(sin)
        val index = sin.readInt()

        return NotificationAction(destination, channel, messageTemplate, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var destination: Destination? = null
        var channel: Channel? = null
        var messageTemplate: Script? = null

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                DESTINATION_FIELD -> destination = Destination.parse(xcp)
                CHANNEL_FIELD -> channel = Channel.parse(xcp)
                MESSAGE_TEMPLATE_FIELD -> messageTemplate = Script.parse(xcp, Script.DEFAULT_TEMPLATE_LANG)
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in NotificationAction.")
            }
        }

        return NotificationAction(
            destination = destination,
            channel = channel,
            messageTemplate = requireNotNull(messageTemplate) { "NotificationAction message template is null" },
            index = index
        )
    }

    override fun getActionType(): String {
        return NotificationAction.name
    }
}
