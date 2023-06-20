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

@file:Suppress("TopLevelPropertyNaming", "MatchingDeclarationName")
package com.colasoft.opensearch.indexmanagement.util

import com.colasoft.opensearch.indexmanagement.common.model.rest.DEFAULT_PAGINATION_FROM
import com.colasoft.opensearch.indexmanagement.common.model.rest.DEFAULT_PAGINATION_SIZE
import com.colasoft.opensearch.indexmanagement.common.model.rest.DEFAULT_QUERY_STRING
import com.colasoft.opensearch.indexmanagement.common.model.rest.DEFAULT_SORT_ORDER
import com.colasoft.opensearch.indexmanagement.common.model.rest.SearchParams
import com.colasoft.opensearch.rest.RestRequest

const val _ID = "_id"
const val NO_ID = ""
const val _VERSION = "_version"
const val _SEQ_NO = "_seq_no"
const val IF_SEQ_NO = "if_seq_no"
const val _PRIMARY_TERM = "_primary_term"
const val IF_PRIMARY_TERM = "if_primary_term"
const val REFRESH = "refresh"

fun RestRequest.getSearchParams(defaultPolicySortField: String): SearchParams {
    val size = this.paramAsInt("size", DEFAULT_PAGINATION_SIZE)
    val from = this.paramAsInt("from", DEFAULT_PAGINATION_FROM)
    val sortField = this.param("sortField", defaultPolicySortField)
    val sortOrder = this.param("sortOrder", DEFAULT_SORT_ORDER)
    val queryString = this.param("queryString", DEFAULT_QUERY_STRING)
    return SearchParams(size, from, sortField, sortOrder, queryString)
}
