/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.model.metric

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.core.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken

class Max() : Metric(Type.MAX) {
    @Suppress("UNUSED_PARAMETER")
    constructor(sin: StreamInput) : this()

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject().startObject(Type.MAX.type).endObject().endObject()
    }

    override fun writeTo(out: StreamOutput) { /* nothing to write */ }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int = javaClass.hashCode()

    override fun toString(): String = "Max()"

    companion object {
        fun parse(xcp: XContentParser): Max {
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp)
            return Max()
        }
    }
}
