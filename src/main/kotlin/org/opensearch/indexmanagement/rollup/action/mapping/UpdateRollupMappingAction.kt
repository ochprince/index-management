/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.action.mapping

import com.colasoft.opensearch.action.ActionType
import com.colasoft.opensearch.action.support.master.AcknowledgedResponse
import com.colasoft.opensearch.common.io.stream.Writeable

class UpdateRollupMappingAction : ActionType<AcknowledgedResponse>(NAME, reader) {

    companion object {
        const val NAME = "cluster:admin/opendistro/rollup/mapping/update"
        val INSTANCE = UpdateRollupMappingAction()
        val reader = Writeable.Reader { AcknowledgedResponse(it) }
    }

    override fun getResponseReader(): Writeable.Reader<AcknowledgedResponse> = reader
}
