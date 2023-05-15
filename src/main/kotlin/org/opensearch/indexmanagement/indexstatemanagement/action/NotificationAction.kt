/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.common.model.notification.Channel
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.destination.Destination
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.notification.AttemptNotificationStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import com.colasoft.opensearch.script.Script

class NotificationAction(
    val destination: Destination?,
    val channel: Channel?,
    val messageTemplate: Script,
    index: Int
) : Action(name, index) {

    init {
        require(destination != null || channel != null) { "Notification must contain a destination or channel" }
        require(destination == null || channel == null) { "Notification can only contain a single destination or channel" }
        require(messageTemplate.lang == MUSTACHE) { "Notification message template must be a mustache script" }
    }

    private val attemptNotificationStep = AttemptNotificationStep(this)
    private val steps = listOf(attemptNotificationStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptNotificationStep
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        if (destination != null) builder.field(DESTINATION_FIELD, destination)
        if (channel != null) builder.field(CHANNEL_FIELD, channel)
        builder.field(MESSAGE_TEMPLATE_FIELD, messageTemplate)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeOptionalWriteable(destination)
        out.writeOptionalWriteable(channel)
        messageTemplate.writeTo(out)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "notification"
        const val DESTINATION_FIELD = "destination"
        const val CHANNEL_FIELD = "channel"
        const val MESSAGE_TEMPLATE_FIELD = "message_template"
        const val MUSTACHE = "mustache"
    }
}
