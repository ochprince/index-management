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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex

import com.colasoft.opensearch.action.support.broadcast.BroadcastRequest
import com.colasoft.opensearch.common.io.stream.StreamInput
import java.io.IOException

@Suppress("SpreadOperator")
class ManagedIndexRequest : BroadcastRequest<ManagedIndexRequest> {

    constructor(vararg indices: String) : super(*indices)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin)
}
