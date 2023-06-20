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

@file:JvmName("NotificationUtils")
package com.colasoft.opensearch.indexmanagement.indexstatemanagement.util

import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.client.node.NodeClient
import com.colasoft.opensearch.commons.authuser.User
import com.colasoft.opensearch.commons.destination.message.LegacyBaseMessage
import com.colasoft.opensearch.commons.notifications.NotificationsPluginInterface
import com.colasoft.opensearch.commons.notifications.action.LegacyPublishNotificationRequest
import com.colasoft.opensearch.commons.notifications.action.LegacyPublishNotificationResponse
import com.colasoft.opensearch.commons.notifications.model.EventSource
import com.colasoft.opensearch.commons.notifications.model.SeverityType
import com.colasoft.opensearch.indexmanagement.common.model.notification.Channel
import com.colasoft.opensearch.indexmanagement.common.model.notification.validateResponseStatus
import com.colasoft.opensearch.indexmanagement.opensearchapi.suspendUntil
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.rest.RestStatus

/**
 * Extension function for publishing a notification to a legacy destination.
 *
 * We now support the new channels from the Notification plugin. But, we still need to support
 * the old embedded legacy destinations that are directly on the policies in the error notifications
 * or notification actions. So we have a separate API in the NotificationsPluginInterface that allows
 * us to publish these old legacy ones directly.
 */
suspend fun LegacyBaseMessage.publishLegacyNotification(client: Client) {
    val baseMessage = this
    val res: LegacyPublishNotificationResponse = NotificationsPluginInterface.suspendUntil {
        this.publishLegacyNotification(
            (client as NodeClient),
            LegacyPublishNotificationRequest(baseMessage),
            it
        )
    }
    validateResponseStatus(RestStatus.fromCode(res.destinationResponse.statusCode), res.destinationResponse.responseContent)
}

/**
 * Extension function for publishing a notification to a channel in the Notification plugin. Builds the event source directly
 * from the managed index metadata.
 */
suspend fun Channel.sendNotification(
    client: Client,
    title: String,
    managedIndexMetaData: ManagedIndexMetaData,
    compiledMessage: String,
    user: User?
) {
    val eventSource = managedIndexMetaData.getEventSource(title)
    this.sendNotification(client, eventSource, compiledMessage, user)
}

fun ManagedIndexMetaData.getEventSource(title: String): EventSource {
    return EventSource(title, indexUuid, SeverityType.INFO)
}
