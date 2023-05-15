/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class AddPolicyAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = AddPolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/add"
    }
}
