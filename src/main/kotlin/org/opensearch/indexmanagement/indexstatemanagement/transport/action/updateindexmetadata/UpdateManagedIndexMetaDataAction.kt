/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.common.io.stream.Writeable

class UpdateManagedIndexMetaDataAction : ActionType<AcknowledgedResponse>(NAME, reader) {

    companion object {
        const val NAME = "cluster:admin/ism/update/managedindexmetadata"
        val INSTANCE = UpdateManagedIndexMetaDataAction()

        val reader = Writeable.Reader { AcknowledgedResponse(it) }
    }

    override fun getResponseReader(): Writeable.Reader<AcknowledgedResponse> = reader
}
