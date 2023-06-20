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
import com.colasoft.opensearch.cluster.metadata.IndexMetadata
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.util.OpenForTesting
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import com.colasoft.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateIndexPriority(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        // if these conditions are false, fail validation and do not execute index_priority action
        if (hasReadOnlyAllowDeleteBlock(indexName)) {
            return this
        }
        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    fun hasReadOnlyAllowDeleteBlock(indexName: String): Boolean {
        val readOnlyAllowDeleteBlock = settings.get(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE)
        if (!readOnlyAllowDeleteBlock.isNullOrEmpty() && readOnlyAllowDeleteBlock.toBoolean()) {
            val message = getReadOnlyAllowDeleteBlockMessage(indexName)
            logger.warn(message)
            validationStatus = ValidationStatus.FAILED
            return true
        }
        return false
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_index_priority"
        fun getReadOnlyAllowDeleteBlockMessage(index: String) = "read_only_allow_delete block is not null for index [index=$index]"
        fun getValidationPassedMessage(index: String) = "Index Priority action validation passed for [index=$index]"
    }
}
