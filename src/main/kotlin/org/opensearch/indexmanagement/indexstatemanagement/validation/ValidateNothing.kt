/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.validation

import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.util.OpenForTesting
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import com.colasoft.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateNothing(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    // skips validation
    override fun execute(indexName: String): Validate {
        return this
    }
}
