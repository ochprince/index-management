/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class ChangePolicyAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = ChangePolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/change"
    }
}
