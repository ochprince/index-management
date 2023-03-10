/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.action.index

import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.action.DocWriteRequest
import com.colasoft.opensearch.action.ValidateActions.addValidationError
import com.colasoft.opensearch.action.index.IndexRequest
import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.index.seqno.SequenceNumbers
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import java.io.IOException

class IndexTransformRequest : IndexRequest {
    var transform: Transform

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        transform = Transform(sin)
        super.setRefreshPolicy(WriteRequest.RefreshPolicy.readFrom(sin))
    }

    constructor(
        transform: Transform,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) {
        this.transform = transform
        if (transform.seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || transform.primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            this.opType(DocWriteRequest.OpType.CREATE)
        } else {
            this.setIfSeqNo(transform.seqNo)
                .setIfPrimaryTerm(transform.primaryTerm)
        }
        super.setRefreshPolicy(refreshPolicy)
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (transform.id.isBlank()) {
            validationException = addValidationError("transformID is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        transform.writeTo(out)
        refreshPolicy.writeTo(out)
    }
}
