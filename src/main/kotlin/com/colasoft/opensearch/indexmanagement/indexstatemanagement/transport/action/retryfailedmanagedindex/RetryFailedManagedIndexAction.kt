/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class RetryFailedManagedIndexAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = RetryFailedManagedIndexAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/retry"
    }
}
