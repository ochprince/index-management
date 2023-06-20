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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import com.colasoft.opensearch.action.ActionType

class ExplainAction private constructor() : ActionType<ExplainResponse>(NAME, ::ExplainResponse) {
    companion object {
        val INSTANCE = ExplainAction()
        const val NAME = "cluster:admin/opendistro/ism/managedindex/explain"
    }
}
