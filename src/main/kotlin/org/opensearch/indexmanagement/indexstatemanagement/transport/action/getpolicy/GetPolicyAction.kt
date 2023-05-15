/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.colasoft.opensearch.action.ActionType

class GetPolicyAction private constructor() : ActionType<GetPolicyResponse>(NAME, ::GetPolicyResponse) {
    companion object {
        val INSTANCE = GetPolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/policy/get"
    }
}
