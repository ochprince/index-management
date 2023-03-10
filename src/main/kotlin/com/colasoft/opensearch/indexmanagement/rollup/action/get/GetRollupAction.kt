/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.get

import com.colasoft.opensearch.action.ActionType

class GetRollupAction private constructor() : ActionType<GetRollupResponse>(NAME, ::GetRollupResponse) {
    companion object {
        val INSTANCE = GetRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/get"
    }
}
