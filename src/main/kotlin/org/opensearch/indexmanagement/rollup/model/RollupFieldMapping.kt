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

package com.colasoft.opensearch.indexmanagement.rollup.model

import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup.Companion.DIMENSIONS_FIELD
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup.Companion.METRICS_FIELD

data class RollupFieldMapping(val fieldType: FieldType, val fieldName: String, val mappingType: String, var sourceType: String? = null) {

    fun sourceType(type: String?) {
        this.sourceType = type
    }

    override fun toString(): String {
        return "$fieldName.$mappingType"
    }

    fun toIssue(isFieldMissing: Boolean = false): String {
        return if (isFieldMissing || mappingType == UNKNOWN_MAPPING) return "missing field $fieldName"
        else when (fieldType) {
            FieldType.METRIC -> "missing $mappingType aggregation on $fieldName"
            else -> "missing $mappingType grouping on $fieldName"
        }
    }

    companion object {
        const val UNKNOWN_MAPPING = "unknown"
        enum class FieldType(val type: String) {
            DIMENSION(DIMENSIONS_FIELD),
            METRIC(METRICS_FIELD);

            override fun toString(): String {
                return type
            }
        }
    }
}
