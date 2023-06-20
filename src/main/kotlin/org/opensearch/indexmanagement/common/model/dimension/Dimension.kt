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

package com.colasoft.opensearch.indexmanagement.common.model.dimension

import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.core.xcontent.ToXContentObject
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.core.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.index.query.AbstractQueryBuilder
import com.colasoft.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import java.io.IOException

abstract class Dimension(
    val type: Type,
    open val sourceField: String,
    open val targetField: String
) : ToXContentObject, Writeable {
    enum class Type(val type: String) {
        DATE_HISTOGRAM("date_histogram"),
        TERMS("terms"),
        HISTOGRAM("histogram");

        override fun toString(): String {
            return type
        }
    }

    abstract fun toSourceBuilder(appendType: Boolean = false): CompositeValuesSourceBuilder<*>

    /**
     * Helper method to get a query which specifies the documents contained within the bucket determined by this dimension.
     *
     * e.g. a terms dimension would return a TermsQueryBuilder specifying just the bucketKey term
     */
    abstract fun toBucketQuery(bucketKey: Any): AbstractQueryBuilder<*>

    /**
     * Helper method that evaluates if the dimension can be realized using mappings provided.
     *
     * e.g. A date_histogram dimension on source_field "a" can only be possible in mappings that contain "date" type field "a".
     */
    abstract fun canBeRealizedInMappings(mappings: Map<String, Any>): Boolean

    companion object {
        const val DIMENSION_SOURCE_FIELD_FIELD = "source_field"
        const val DIMENSION_TARGET_FIELD_FIELD = "target_field"

        @Suppress("ComplexMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Dimension {
            var dimension: Dimension? = null
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                dimension = when (fieldName) {
                    Type.DATE_HISTOGRAM.type -> DateHistogram.parse(xcp)
                    Type.TERMS.type -> Terms.parse(xcp)
                    Type.HISTOGRAM.type -> Histogram.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid dimension type [$fieldName] found in dimensions")
                }
            }

            return requireNotNull(dimension) { "Dimension cannot be null" }
        }
    }
}
