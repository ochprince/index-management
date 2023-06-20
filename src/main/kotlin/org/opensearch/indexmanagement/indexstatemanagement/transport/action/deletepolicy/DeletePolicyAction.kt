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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.action.delete.DeleteResponse

class DeletePolicyAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeletePolicyAction()
        const val NAME = "cluster:admin/opendistro/ism/policy/delete"
    }
}
