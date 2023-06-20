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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import com.colasoft.opensearch.action.ActionType

class IndexPolicyAction private constructor() : ActionType<IndexPolicyResponse>(NAME, ::IndexPolicyResponse) {
    companion object {
        val INSTANCE = IndexPolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/policy/write"
    }
}
