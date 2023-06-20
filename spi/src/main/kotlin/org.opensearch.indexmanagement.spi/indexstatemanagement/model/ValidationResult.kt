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

package com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model

import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.common.xcontent.LoggingDeprecationHandler
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.ToXContentFragment
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParserUtils
import com.colasoft.opensearch.common.xcontent.XContentType
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Locale

data class ValidationResult(
    val validationMessage: String,
    val validationStatus: Validate.ValidationStatus
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(validationMessage)
        validationStatus.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .field(VALIDATION_MESSAGE, validationMessage)
            .field(VALIDATION_STATUS, validationStatus.toString())
        return builder
    }

    fun getMapValueString(): String {
        return Strings.toString(XContentType.JSON, this)
    }

    companion object {
        const val VALIDATE = "validate"
        const val VALIDATION_MESSAGE = "validation_message"
        const val VALIDATION_STATUS = "validation_status"

        fun fromStreamInput(si: StreamInput): ValidationResult {
            val validationMessage: String? = si.readString()
            val validationStatus: Validate.ValidationStatus? = Validate.ValidationStatus.read(si)

            return ValidationResult(
                requireNotNull(validationMessage) { "$VALIDATION_MESSAGE is null" },
                requireNotNull(validationStatus) { "$VALIDATION_STATUS is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): ValidationResult? {
            val stepJsonString = map[VALIDATE]
            return if (stepJsonString != null) {
                val inputStream = ByteArrayInputStream(stepJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): ValidationResult {
            var validationMessage: String? = null
            var validationStatus: Validate.ValidationStatus? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    VALIDATION_MESSAGE -> validationMessage = xcp.text()
                    VALIDATION_STATUS -> validationStatus = Validate.ValidationStatus.valueOf(xcp.text().uppercase(Locale.ROOT))
                }
            }

            return ValidationResult(
                requireNotNull(validationMessage) { "$VALIDATION_MESSAGE is null" },
                requireNotNull(validationStatus) { "$VALIDATION_STATUS is null" }
            )
        }
    }
}
