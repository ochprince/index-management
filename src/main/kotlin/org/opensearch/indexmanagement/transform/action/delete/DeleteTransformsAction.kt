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

package com.colasoft.opensearch.indexmanagement.transform.action.delete

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.action.bulk.BulkResponse

class DeleteTransformsAction private constructor() : ActionType<BulkResponse>(NAME, ::BulkResponse) {
    companion object {
        val INSTANCE = DeleteTransformsAction()
        const val NAME = "cluster:admin/opendistro/transform/delete"
    }
}
