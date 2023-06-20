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

package com.colasoft.opensearch.indexmanagement.transform.action.delete

import com.colasoft.opensearch.action.ActionRequest
import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.action.ValidateActions.addValidationError
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class DeleteTransformsRequest(
    val ids: List<String>,
    val force: Boolean
) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        ids = sin.readStringList(),
        force = sin.readBoolean()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (ids.isEmpty()) {
            validationException = addValidationError("List of ids to delete is empty", validationException)
        }

        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(ids)
        out.writeBoolean(force)
    }

    companion object {
        const val DEFAULT_FORCE_DELETE = false
    }
}
