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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.util

import com.nhaarman.mockitokotlin2.mock
import org.mockito.Mockito
import com.colasoft.opensearch.action.admin.cluster.node.stats.NodeStats
import com.colasoft.opensearch.cluster.routing.allocation.DiskThresholdSettings
import com.colasoft.opensearch.common.settings.ClusterSettings
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.unit.ByteSizeValue
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomByteSizeValue
import com.colasoft.opensearch.indexmanagement.randomInstant
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import com.colasoft.opensearch.jobscheduler.spi.LockModel
import com.colasoft.opensearch.monitor.fs.FsInfo
import com.colasoft.opensearch.test.OpenSearchTestCase

class StepUtilsTests : OpenSearchTestCase() {

    fun `test get shrink lock model`() {
        val shrinkActionProperties = ShrinkActionProperties(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomInstant().toEpochMilli(),
            randomInstant().toEpochMilli(),
            mapOf()
        )
        val lockModel = getShrinkLockModel(shrinkActionProperties)
        assertEquals("Incorrect lock model job index name", INDEX_MANAGEMENT_INDEX, lockModel.jobIndexName)
        assertEquals("Incorrect lock model jobID", getShrinkJobID(shrinkActionProperties.nodeName), lockModel.jobId)
        assertEquals("Incorrect lock model duration", shrinkActionProperties.lockDurationSecond, lockModel.lockDurationSeconds)
        assertEquals("Incorrect lock model lockID", "${lockModel.jobIndexName}-${lockModel.jobId}", lockModel.lockId)
        assertEquals("Incorrect lock model sequence number", shrinkActionProperties.lockSeqNo, lockModel.seqNo)
        assertEquals("Incorrect lock model primary term", shrinkActionProperties.lockPrimaryTerm, lockModel.primaryTerm)
        assertEquals("Lock should not be expired when created", false, lockModel.isExpired)
        assertEquals("Lock should not be released when created", false, lockModel.isReleased)
    }

    fun `test get updated shrink action properties`() {
        val shrinkActionProperties = ShrinkActionProperties(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomInstant().toEpochMilli(),
            randomInstant().toEpochMilli(),
            mapOf()
        )
        val lockModel = LockModel(
            randomAlphaOfLength(10),
            getShrinkLockID(shrinkActionProperties.nodeName),
            randomInstant(),
            randomInstant().toEpochMilli(),
            false,
            randomNonNegativeLong(),
            randomNonNegativeLong()
        )
        val updatedProperties = getUpdatedShrinkActionProperties(shrinkActionProperties, lockModel)

        assertEquals("Node name should not have updated", updatedProperties.nodeName, shrinkActionProperties.nodeName)
        assertEquals("Index name should not have updated", updatedProperties.targetIndexName, shrinkActionProperties.targetIndexName)
        assertEquals("Num shards should not have updated", updatedProperties.targetNumShards, shrinkActionProperties.targetNumShards)
        assertEquals("Settings should not have updated", updatedProperties.originalIndexSettings, shrinkActionProperties.originalIndexSettings)
        assertEquals("Lock sequence number should have updated", updatedProperties.lockSeqNo, lockModel.seqNo)
        assertEquals("Lock primary term should have updated", updatedProperties.lockPrimaryTerm, lockModel.primaryTerm)
        assertEquals("Lock epoch time should have updated", updatedProperties.lockEpochSecond, lockModel.lockTime.epochSecond)
        assertEquals("Lock duration should have updated", updatedProperties.lockDurationSecond, lockModel.lockDurationSeconds)
    }

    fun `test get action start time`() {
        val metadata = ManagedIndexMetaData(
            "indexName", "indexUuid", "policy_id", null, null, null, null, null, null, null,
            ActionMetaData("name", randomInstant().toEpochMilli(), 0, false, 0, null, null), null, null, null
        )
        assertEquals("Action start time was not extracted correctly", metadata.actionMetaData?.startTime, getActionStartTime(metadata).toEpochMilli())
    }

    fun `test get free bytes threshold high`() {
        val settings = Settings.builder()
        val nodeBytes = randomByteSizeValue().bytes
        val expected: Long = if (randomBoolean()) {
            val bytes = randomLongBetween(10, 100000000)
            val highDisk = ByteSizeValue(bytes).stringRep
            val lowDisk = ByteSizeValue(bytes + 1).stringRep
            val floodDisk = ByteSizeValue(bytes - 1).stringRep
            settings.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key, highDisk)
            settings.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key, lowDisk)
            settings.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key, floodDisk)
            bytes
        } else {
            val percentage = randomDoubleBetween(0.005, 0.995, false)
            settings.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key, "${percentage * 100}%")
            settings.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key, "${(percentage - 0.001) * 100}%")
            settings.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key, "${(percentage + 0.001) * 100}%")
            (nodeBytes * (1 - percentage)).toLong()
        }
        val clusterSettings = ClusterSettings(settings.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        val thresholdHigh = getFreeBytesThresholdHigh(clusterSettings, nodeBytes)
        assertEquals(expected, thresholdHigh)
    }

    fun `test free memory after shrink`() {
        val nodeStats: NodeStats = mock()
        val fsInfo: FsInfo = mock()
        Mockito.`when`(nodeStats.fs).thenReturn(fsInfo)
        val path: FsInfo.Path = mock()
        Mockito.`when`(fsInfo.total).thenReturn(path)
        val totalBytes = randomLongBetween(10, 100000000)
        val freeBytes = randomLongBetween(0, totalBytes)
        val indexSize = randomLongBetween(0, totalBytes / 2)
        val threshold = randomLongBetween(0, totalBytes / 2)
        Mockito.`when`(path.free).thenReturn(ByteSizeValue(freeBytes))
        Mockito.`when`(path.total).thenReturn(ByteSizeValue(totalBytes))
        val settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key, ByteSizeValue(threshold).stringRep)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key, ByteSizeValue(threshold + 1).stringRep)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key, ByteSizeValue(threshold - 1).stringRep)
        val clusterSettings = ClusterSettings(settings.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        val remainingSpace = freeBytes - ((2 * indexSize) + threshold)
        if (remainingSpace > 0) {
            assertEquals(remainingSpace, getNodeFreeDiskSpaceAfterShrink(nodeStats, indexSize, clusterSettings))
        } else {
            assertEquals(-1L, getNodeFreeDiskSpaceAfterShrink(nodeStats, indexSize, clusterSettings))
        }
    }
}
