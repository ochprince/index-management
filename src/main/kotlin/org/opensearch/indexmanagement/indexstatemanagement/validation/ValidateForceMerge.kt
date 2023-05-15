/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.util.OpenForTesting
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import com.colasoft.opensearch.indexmanagement.transform.settings.TransformSettings
import com.colasoft.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateForceMerge(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        if (!dataSizeNotLarge(indexName)) {
            return this
        }
        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    fun dataSizeNotLarge(indexName: String): Boolean {
        val circuitBreakerEnabled = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED.get(settings)
        val circuitBreakerJvmThreshold = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD.get(settings)
        if (circuitBreakerEnabled && jvmService.stats().mem.heapUsedPercent > circuitBreakerJvmThreshold) {
            val message = getFailedDataTooLargeMessage(indexName)
            logger.warn(message)
            validationStatus = ValidationStatus.RE_VALIDATING
            return false
        }
        return true
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_force_merge"
        fun getFailedDataTooLargeMessage(index: String) = "Data too large and is over the allowed limit for index [index=$index]"
        fun getValidationPassedMessage(index: String) = "Force merge validation passed for [index=$index]"
    }
}
