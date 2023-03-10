/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.explain

import com.colasoft.opensearch.action.ActionType

class ExplainTransformAction private constructor() : ActionType<ExplainTransformResponse>(NAME, ::ExplainTransformResponse) {
    companion object {
        val INSTANCE = ExplainTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/explain"
    }
}
