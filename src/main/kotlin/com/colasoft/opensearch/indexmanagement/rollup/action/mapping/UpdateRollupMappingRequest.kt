/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.mapping

import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.action.support.master.AcknowledgedRequest
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup

class UpdateRollupMappingRequest : AcknowledgedRequest<UpdateRollupMappingRequest> {
    val rollup: Rollup

    constructor(sin: StreamInput) : super(sin) {
        rollup = Rollup(sin)
    }

    constructor(rollup: Rollup) {
        this.rollup = rollup
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        rollup.writeTo(out)
    }
}
