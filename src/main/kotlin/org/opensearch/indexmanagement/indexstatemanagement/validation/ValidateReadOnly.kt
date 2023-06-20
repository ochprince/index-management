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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import com.colasoft.opensearch.indexmanagement.transform.settings.TransformSettings
import com.colasoft.opensearch.indexmanagement.util.OpenForTesting
import com.colasoft.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateReadOnly(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        // if these conditions are false, fail validation and do not execute read_only action
        if (dataSizeTooLarge(indexName) || hasReadOnlyAllowDeleteBlock(indexName)) {
            return this
        }
        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    fun hasReadOnlyAllowDeleteBlock(indexName: String): Boolean {
        val readOnlyAllowDeleteBlock = settings.get(settingKey)
        if (!readOnlyAllowDeleteBlock.isNullOrEmpty()) {
            val message = getReadOnlyAllowDeleteBlockMessage(indexName)
            logger.warn(message)
            validationStatus = ValidationStatus.RE_VALIDATING
            return true
        }
        return false
    }

    fun dataSizeTooLarge(indexName: String): Boolean {
        val circuitBreakerEnabled = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED.get(settings)
        val circuitBreakerJvmThreshold = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD.get(settings)
        if (circuitBreakerEnabled && jvmService.stats().mem.heapUsedPercent > circuitBreakerJvmThreshold) {
            val message = getFailedDataTooLargeMessage(indexName)
            logger.warn(message)
            validationStatus = ValidationStatus.RE_VALIDATING
            return true
        }
        return false
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_read_only"
        const val settingKey = "read_only_allow_delete"
        fun getReadOnlyAllowDeleteBlockMessage(index: String) = "read_only_allow_delete block is not null for index [index=$index]"
        fun getFailedDataTooLargeMessage(index: String) = "Data too large and is over the allowed limit for index [index=$index]"
        fun getValidationPassedMessage(index: String) = "validate_read_only action validation passed for [index=$index]"
    }
}
