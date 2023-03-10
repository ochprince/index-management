/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.start

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse

class StartRollupAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        val INSTANCE = StartRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/start"
    }
}
