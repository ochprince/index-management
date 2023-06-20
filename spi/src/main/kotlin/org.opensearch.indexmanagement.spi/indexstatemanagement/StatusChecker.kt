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

package com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement

import com.colasoft.opensearch.cluster.ClusterState

interface StatusChecker {

    /**
     * checks and returns the status of the extension
     */
    fun check(clusterState: ClusterState): Status {
        return Status.ENABLED
    }
}

enum class Status(private val value: String) {
    ENABLED("enabled"),
    DISABLED("disabled");

    override fun toString(): String {
        return value
    }
}

class DefaultStatusChecker : StatusChecker
