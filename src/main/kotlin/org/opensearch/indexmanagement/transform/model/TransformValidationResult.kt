/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.model

data class TransformValidationResult(val isValid: Boolean, val issues: List<String> = listOf())
