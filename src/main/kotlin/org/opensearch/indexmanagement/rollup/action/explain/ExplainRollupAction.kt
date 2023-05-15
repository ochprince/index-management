/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.explain

import com.colasoft.opensearch.action.ActionType

class ExplainRollupAction private constructor() : ActionType<ExplainRollupResponse>(NAME, ::ExplainRollupResponse) {
    companion object {
        val INSTANCE = ExplainRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/explain"
    }
}
