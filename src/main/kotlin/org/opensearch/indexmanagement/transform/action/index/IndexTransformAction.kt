/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.index

import com.colasoft.opensearch.action.ActionType

class IndexTransformAction private constructor() : ActionType<IndexTransformResponse>(NAME, ::IndexTransformResponse) {
    companion object {
        val INSTANCE = IndexTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/index"
    }
}
