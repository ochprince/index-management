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
package com.colasoft.opensearch.indexmanagement.snapshotmanagement.util

import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.NAME_FIELD
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_TYPE
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.validateSMPolicyName
import com.colasoft.opensearch.rest.RestRequest

const val SM_POLICY_NAME_KEYWORD = "$SM_TYPE.$NAME_FIELD"
const val DEFAULT_SM_POLICY_SORT_FIELD = SM_POLICY_NAME_KEYWORD

fun RestRequest.getValidSMPolicyName(): String {
    val policyName = this.param("policyName", "")
    validateSMPolicyName(policyName)
    return policyName
}
