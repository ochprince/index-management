/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class RemovePolicyAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = RemovePolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/remove"
    }
}
