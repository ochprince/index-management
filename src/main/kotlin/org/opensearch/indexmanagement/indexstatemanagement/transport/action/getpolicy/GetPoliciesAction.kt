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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.colasoft.opensearch.action.ActionType

class GetPoliciesAction private constructor() : ActionType<GetPoliciesResponse>(NAME, ::GetPoliciesResponse) {
    companion object {
        val INSTANCE = GetPoliciesAction()
        const val NAME = "cluster:admin/opendistro/ism/policy/search"
    }
}
