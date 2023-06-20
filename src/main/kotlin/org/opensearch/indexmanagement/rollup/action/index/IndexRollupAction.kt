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

package com.colasoft.opensearch.indexmanagement.rollup.action.index

import com.colasoft.opensearch.action.ActionType

class IndexRollupAction private constructor() : ActionType<IndexRollupResponse>(NAME, ::IndexRollupResponse) {
    companion object {
        val INSTANCE = IndexRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/index"
    }
}
