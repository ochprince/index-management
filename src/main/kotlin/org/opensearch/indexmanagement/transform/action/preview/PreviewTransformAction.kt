/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.preview

import com.colasoft.opensearch.action.ActionType

class PreviewTransformAction private constructor() : ActionType<PreviewTransformResponse>(NAME, ::PreviewTransformResponse) {
    companion object {
        val INSTANCE = PreviewTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/preview"
    }
}
