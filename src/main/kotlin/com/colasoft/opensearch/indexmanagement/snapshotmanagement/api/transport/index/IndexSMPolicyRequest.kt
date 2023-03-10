/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.action.DocWriteRequest
import com.colasoft.opensearch.action.ValidateActions
import com.colasoft.opensearch.action.index.IndexRequest
import com.colasoft.opensearch.action.support.WriteRequest
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.index.seqno.SequenceNumbers
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import java.time.Instant.now

class IndexSMPolicyRequest : IndexRequest {

    var policy: SMPolicy

    constructor(
        policy: SMPolicy,
        create: Boolean,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) : super() {
        this.policy = policy
        this.create(create)
        if (policy.seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO && policy.primaryTerm != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            this.setIfSeqNo(policy.seqNo).setIfPrimaryTerm(policy.primaryTerm)
            this.policy = policy.copy(jobLastUpdateTime = now())
        }
        this.refreshPolicy = refreshPolicy
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        val invalidSeqNumPrimaryTerm = this.ifSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO ||
            this.ifPrimaryTerm() == SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        if (this.opType() != DocWriteRequest.OpType.CREATE && invalidSeqNumPrimaryTerm) {
            validationException = ValidateActions.addValidationError(SEQ_NUM_PRIMARY_TERM_UPDATE_ERROR, validationException)
        }
        return validationException
    }

    constructor(sin: StreamInput) : super(sin) {
        this.policy = SMPolicy(sin)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        policy.writeTo(out)
    }

    companion object {
        private const val SEQ_NUM_PRIMARY_TERM_UPDATE_ERROR =
            "Sequence number and primary term must be provided when updating a snapshot management policy"
    }
}
