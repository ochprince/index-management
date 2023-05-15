/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.explain

import com.colasoft.opensearch.action.ActionRequest
import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.action.ValidateActions.addValidationError
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class ExplainTransformRequest(val transformIDs: List<String>) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(transformIDs = sin.readStringArray().toList())

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (transformIDs.isEmpty()) {
            validationException = addValidationError("Missing transformID", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(transformIDs.toTypedArray())
    }
}
