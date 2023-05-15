/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.start

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse

class StartTransformAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        val INSTANCE = StartTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/start"
    }
}
