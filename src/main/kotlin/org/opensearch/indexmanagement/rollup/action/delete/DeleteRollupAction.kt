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

package com.colasoft.opensearch.indexmanagement.rollup.action.delete

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.action.delete.DeleteResponse

class DeleteRollupAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/delete"
    }
}
