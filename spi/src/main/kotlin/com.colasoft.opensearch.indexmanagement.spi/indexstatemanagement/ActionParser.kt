/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.xcontent.XContentParser

abstract class ActionParser(var customAction: Boolean = false) {

    /**
     * The action type parser will parse
     */
    abstract fun getActionType(): String

    /**
     * Deserialize Action from stream input
     */
    abstract fun fromStreamInput(sin: StreamInput): Action

    /**
     * Deserialize Action from xContent
     */
    abstract fun fromXContent(xcp: XContentParser, index: Int): Action
}
