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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.destination

import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.core.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

/**
 * A value object that represents a Chime message. Chime message will be
 * submitted to the Chime destination
 *
 * Temporary import from alerting, this will be removed once we pull notifications out of
 * alerting so all plugins can consume and use.
 */
data class Chime(val url: String) : ToXContent, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject(TYPE)
            .field(URL, url)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(url)
    }

    companion object {
        const val URL = "url"
        const val TYPE = "chime"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Chime {
            var url: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    URL -> url = xcp.text()
                    else -> {
                        error("Unexpected field: $fieldName, while parsing Chime destination")
                    }
                }
            }
            return Chime(requireNotNull(url) { "URL is null or empty" })
        }
    }

    // Complete JSON structure is now constructed in the notification plugin
    fun constructMessageContent(subject: String?, message: String): String {
        return if (Strings.isNullOrEmpty(subject)) message else "$subject \n\n $message"
    }
}
