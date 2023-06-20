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

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.core.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.index.query.AbstractQueryBuilder
import com.colasoft.opensearch.index.query.TermsQueryBuilder
import com.colasoft.opensearch.indexmanagement.util.IndexUtils.Companion.getFieldFromMappings
import com.colasoft.opensearch.search.aggregations.AggregatorFactories
import com.colasoft.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import com.colasoft.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import com.colasoft.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import java.io.IOException

data class Terms(
    override val sourceField: String,
    override val targetField: String
) : Dimension(Type.TERMS, sourceField, targetField) {

    init {
        require(sourceField.isNotEmpty() && targetField.isNotEmpty()) { "Source and target field must not be empty" }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sourceField = sin.readString(),
        targetField = sin.readString()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(type.type)
            .field(DIMENSION_SOURCE_FIELD_FIELD, sourceField)
            .field(DIMENSION_TARGET_FIELD_FIELD, targetField)
            .endObject()
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(sourceField)
        out.writeString(targetField)
    }

    override fun toSourceBuilder(appendType: Boolean): CompositeValuesSourceBuilder<*> {
        val name = if (appendType) "${this.targetField}.${Type.TERMS.type}" else this.targetField
        return TermsValuesSourceBuilder(name)
            .missingBucket(true)
            .field(this.sourceField)
    }

    override fun toBucketQuery(bucketKey: Any): AbstractQueryBuilder<*> {
        return TermsQueryBuilder(sourceField, bucketKey)
    }

    override fun canBeRealizedInMappings(mappings: Map<String, Any>): Boolean {
        val fieldType = getFieldFromMappings(sourceField, mappings)?.get("type") ?: return false

        // TODO: This is incomplete as more than keywords can be grouped as terms, need to figure out the correct way to do this check for now just
        //  checking the types that are not
        return "text" != fieldType
    }

    // TODO missing terms field
    fun getRewrittenAggregation(
        aggregationBuilder: TermsAggregationBuilder,
        subAggregations: AggregatorFactories.Builder
    ): TermsAggregationBuilder =
        TermsAggregationBuilder(aggregationBuilder.name)
            .also { aggregationBuilder.collectMode()?.apply { it.collectMode(this) } }
            .executionHint(aggregationBuilder.executionHint())
            .includeExclude(aggregationBuilder.includeExclude())
            .also {
                if (aggregationBuilder.minDocCount() >= 0) {
                    it.minDocCount(aggregationBuilder.minDocCount())
                }
            }
            .also { aggregationBuilder.order()?.apply { it.order(this) } }
            .also {
                if (aggregationBuilder.shardMinDocCount() >= 0) {
                    it.shardMinDocCount(aggregationBuilder.shardMinDocCount())
                }
            }
            .also {
                if (aggregationBuilder.shardSize() > 0) {
                    it.shardSize(aggregationBuilder.shardSize())
                }
            }
            .showTermDocCountError(aggregationBuilder.showTermDocCountError())
            .also {
                if (aggregationBuilder.size() > 0) {
                    it.size(aggregationBuilder.size())
                }
            }
            .field(this.targetField + ".terms")
            .subAggregations(subAggregations)

    companion object {
        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Terms {
            var sourceField: String? = null
            var targetField: String? = null

            ensureExpectedToken(
                Token.START_OBJECT,
                xcp.currentToken(),
                xcp
            )
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    DIMENSION_SOURCE_FIELD_FIELD -> sourceField = xcp.text()
                    DIMENSION_TARGET_FIELD_FIELD -> targetField = xcp.text()
                    else -> throw IllegalArgumentException("Invalid field [$fieldName] found in terms dimension.")
                }
            }
            if (targetField == null) targetField = sourceField
            return Terms(
                requireNotNull(sourceField) { "Source field cannot be null" },
                requireNotNull(targetField) { "Target field cannot be null" }
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = Terms(sin)
    }
}
