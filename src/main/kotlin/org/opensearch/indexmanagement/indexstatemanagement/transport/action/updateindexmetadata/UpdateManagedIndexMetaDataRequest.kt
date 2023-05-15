/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata

import com.colasoft.opensearch.action.ActionRequestValidationException
import com.colasoft.opensearch.action.ValidateActions.addValidationError
import com.colasoft.opensearch.action.support.master.AcknowledgedRequest
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.index.Index
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData

class UpdateManagedIndexMetaDataRequest : AcknowledgedRequest<UpdateManagedIndexMetaDataRequest> {

    var indicesToAddManagedIndexMetaDataTo: List<Pair<Index, ManagedIndexMetaData>>
        private set

    var indicesToRemoveManagedIndexMetaDataFrom: List<Index>
        private set

    constructor(si: StreamInput) : super(si) {
        indicesToAddManagedIndexMetaDataTo = si.readList {
            val index = Index(it)
            val managedIndexMetaData = ManagedIndexMetaData.fromStreamInput(it)
            Pair(index, managedIndexMetaData)
        }

        indicesToRemoveManagedIndexMetaDataFrom = si.readList { Index(it) }
    }

    constructor(
        indicesToAddManagedIndexMetaDataTo: List<Pair<Index, ManagedIndexMetaData>> = listOf(),
        indicesToRemoveManagedIndexMetaDataFrom: List<Index> = listOf()
    ) {
        this.indicesToAddManagedIndexMetaDataTo = indicesToAddManagedIndexMetaDataTo
        this.indicesToRemoveManagedIndexMetaDataFrom = indicesToRemoveManagedIndexMetaDataFrom
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null

        if (this.indicesToAddManagedIndexMetaDataTo.isEmpty() && this.indicesToRemoveManagedIndexMetaDataFrom.isEmpty()) {
            validationException = addValidationError(
                "At least one non-empty List must be given for UpdateManagedIndexMetaDataRequest",
                validationException
            )
        }

        return validationException
    }

    override fun writeTo(streamOutput: StreamOutput) {
        super.writeTo(streamOutput)

        streamOutput.writeCollection(indicesToAddManagedIndexMetaDataTo) { so, pair ->
            pair.first.writeTo(so)
            pair.second.writeTo(so)
        }

        streamOutput.writeCollection(indicesToRemoveManagedIndexMetaDataFrom) { so, index ->
            index.writeTo(so)
        }
    }
}
