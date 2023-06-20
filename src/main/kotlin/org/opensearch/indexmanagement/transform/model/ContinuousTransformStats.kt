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

package com.colasoft.opensearch.indexmanagement.transform.model

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.ToXContentObject
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParserUtils
import com.colasoft.opensearch.indexmanagement.opensearchapi.instant
import java.io.IOException
import java.time.Instant

data class ContinuousTransformStats(
    val lastTimestamp: Instant?,
    val documentsBehind: Map<String, Long>?
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        lastTimestamp = if (sin.readBoolean()) sin.readInstant() else null,
        documentsBehind = if (sin.readBoolean()) sin.readMap({ it.readString() }, { it.readLong() }) else null
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (lastTimestamp != null) builder.timeField(LAST_TIMESTAMP_FIELD, LAST_TIMESTAMP_FIELD_IN_MILLIS, lastTimestamp.toEpochMilli())
        if (documentsBehind != null) builder.field(DOCUMENTS_BEHIND_FIELD, documentsBehind)
        return builder.endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(lastTimestamp != null)
        lastTimestamp?.let { out.writeInstant(it) }
        out.writeBoolean(documentsBehind != null)
        documentsBehind?.let { out.writeMap(it, { writer, k -> writer.writeString(k) }, { writer, v -> writer.writeLong(v) }) }
    }

    companion object {
        private const val LAST_TIMESTAMP_FIELD = "last_timestamp"
        private const val LAST_TIMESTAMP_FIELD_IN_MILLIS = "last_timestamp_in_millis"
        private const val DOCUMENTS_BEHIND_FIELD = "documents_behind"

        @Suppress("ComplexMethod, LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ContinuousTransformStats {
            var lastTimestamp: Instant? = null
            var documentsBehind: Map<String, Long>? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    LAST_TIMESTAMP_FIELD -> lastTimestamp = xcp.instant()
                    DOCUMENTS_BEHIND_FIELD -> documentsBehind = xcp.map({ HashMap<String, Long>() }, { parser -> parser.longValue() })
                }
            }

            return ContinuousTransformStats(
                lastTimestamp = lastTimestamp,
                documentsBehind = documentsBehind,
            )
        }
    }
}
