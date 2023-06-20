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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.model

import com.colasoft.opensearch.cluster.routing.allocation.DiskThresholdSettings
import com.colasoft.opensearch.common.io.stream.InputStreamStreamInput
import com.colasoft.opensearch.common.io.stream.OutputStreamStreamOutput
import com.colasoft.opensearch.common.settings.ClusterSettings
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.unit.ByteSizeValue
import com.colasoft.opensearch.common.unit.TimeValue
import com.colasoft.opensearch.common.xcontent.LoggingDeprecationHandler
import com.colasoft.opensearch.common.xcontent.XContentFactory
import com.colasoft.opensearch.common.xcontent.XContentType
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomAllocationActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomChannel
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomByteSizeValue
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomCloseActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomDeleteActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomDestination
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomForceMergeActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomIndexPriorityActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomNotificationActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomOpenActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomReadWriteActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomRolloverActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomRollupActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomShrinkAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomSnapshotActionConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomTemplateScript
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomTimeValueObject
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.getFreeBytesThresholdHigh
import com.colasoft.opensearch.indexmanagement.opensearchapi.convertToMap
import com.colasoft.opensearch.indexmanagement.opensearchapi.string
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
import com.colasoft.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.lang.Math.abs
import kotlin.test.assertFailsWith

class ActionTests : OpenSearchTestCase() {

    fun `test invalid timeout for delete action fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for invalid timeout") {
            ActionTimeout(timeout = TimeValue.parseTimeValue("invalidTimeout", "timeout_test"))
        }
    }

    fun `test action retry count of -1 fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for retry count less than 1") {
            ActionRetry(count = -1)
        }
    }

    fun `test rollover action minimum size of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for minSize less than 1") {
            randomRolloverActionConfig(minSize = ByteSizeValue.parseBytesSizeValue("0", "min_size_test"))
        }
    }

    fun `test rollover action minimum doc count of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for minDoc less than 1") {
            randomRolloverActionConfig(minDocs = 0)
        }
    }

    fun `test force merge action max num segments of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for maxNumSegments less than 1") {
            randomForceMergeActionConfig(maxNumSegments = 0)
        }
    }

    fun `test shrink action multiple shard options fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for multiple shard options used") {
            randomShrinkAction(3, randomByteSizeValue(), .30)
        }
    }

    fun `test allocation action empty parameters fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for empty parameters") {
            randomAllocationActionConfig()
        }
    }

    fun `test set read write action round trip`() {
        roundTripAction(randomReadWriteActionConfig())
    }

    fun `test set read only action round trip`() {
        roundTripAction(randomReadOnlyActionConfig())
    }

    fun `test notification having both channel and destination fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for notification using both destination and channel") {
            NotificationAction(destination = randomDestination(), channel = randomChannel(), messageTemplate = randomTemplateScript(), index = 0)
        }
    }

    fun `test rollover action round trip`() {
        roundTripAction(randomRolloverActionConfig())
    }

    fun `test rollup action round trip`() {
        roundTripAction(randomRollupActionConfig())
    }

    fun `test replica count action round trip`() {
        roundTripAction(randomReplicaCountActionConfig())
    }

    fun `test force merge action round trip`() {
        roundTripAction(randomForceMergeActionConfig())
    }

    fun `test notification action round trip`() {
        roundTripAction(randomNotificationActionConfig())
    }

    fun `test snapshot action round trip`() {
        roundTripAction(randomSnapshotActionConfig(snapshot = "snapshot", repository = "repository"))
    }

    fun `test index priority action round trip`() {
        roundTripAction(randomIndexPriorityActionConfig())
    }

    fun `test allocation action round trip`() {
        roundTripAction(
            randomAllocationActionConfig
            (
                require = mapOf("box_type" to "hot"),
                include = mapOf(randomAlphaOfLengthBetween(1, 10) to randomAlphaOfLengthBetween(1, 10)),
                exclude = mapOf(randomAlphaOfLengthBetween(1, 10) to randomAlphaOfLengthBetween(1, 10))
            )
        )
    }

    fun `test close action round trip`() {
        roundTripAction(randomCloseActionConfig())
    }

    fun `test open action round trip`() {
        roundTripAction(randomOpenActionConfig())
    }

    fun `test delete action round trip`() {
        roundTripAction(randomDeleteActionConfig())
    }

    fun `test shrink action round trip`() {
        roundTripAction(randomShrinkAction())
    }

    fun `test action timeout and retry round trip`() {
        val builder = XContentFactory.jsonBuilder()
            .startObject()
            .field(ActionTimeout.TIMEOUT_FIELD, randomTimeValueObject().stringRep)
            .startObject(ActionRetry.RETRY_FIELD)
            .field(ActionRetry.COUNT_FIELD, 1)
            .field(ActionRetry.BACKOFF_FIELD, ActionRetry.Backoff.EXPONENTIAL)
            .field(ActionRetry.DELAY_FIELD, TimeValue.timeValueMinutes(1))
            .endObject()
            .startObject(DeleteAction.name)
            .endObject()
            .endObject()

        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, builder.string())
        parser.nextToken()

        val action = ISMActionsParser.instance.parse(parser, 1)
        roundTripAction(action)
    }

    fun `test shrink disk threshold percentage settings`() {
        val rawPercentage = randomIntBetween(0, 100)
        val percentage = "$rawPercentage%"
        val settings = Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key, percentage)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key, percentage)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key, percentage).build()
        val clusterSettings = ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.map { it }.toSet())
        val totalNodeBytes = randomByteSizeValue().bytes
        val thresholdBytes = getFreeBytesThresholdHigh(clusterSettings, totalNodeBytes)
        val expectedThreshold: Long = ((1 - (rawPercentage.toDouble() / 100.0)) * totalNodeBytes).toLong()
        // To account for some rounding issues, allow an error of 1
        val approximatelyEqual = kotlin.math.abs(thresholdBytes - expectedThreshold) <= 1
        assertTrue("Free bytes threshold not being calculated correctly for percentage setting.", approximatelyEqual)
    }

    fun `test shrink disk threshold byte settings`() {
        val byteValue = randomByteSizeValue()
        val settings = Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key, byteValue)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key, byteValue)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key, byteValue).build()
        val clusterSettings = ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.map { it }.toSet())
        val thresholdBytes = getFreeBytesThresholdHigh(clusterSettings, randomByteSizeValue().bytes)
        assertEquals("Free bytes threshold not being calculated correctly for byte setting.", thresholdBytes, byteValue.bytes)
    }

    private fun roundTripAction(expectedAction: Action) {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        expectedAction.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))

        val actualAction = ISMActionsParser.instance.fromStreamInput(input)
        assertEquals(expectedAction.convertToMap(), actualAction.convertToMap())
    }
}
