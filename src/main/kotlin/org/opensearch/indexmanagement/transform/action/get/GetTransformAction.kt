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

package com.colasoft.opensearch.indexmanagement.transform.action.get

import com.colasoft.opensearch.action.ActionType

class GetTransformAction private constructor() : ActionType<GetTransformResponse>(NAME, ::GetTransformResponse) {
    companion object {
        val INSTANCE = GetTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/get"
    }
}
