/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.common.model.dimension

import org.junit.Assert
import com.colasoft.opensearch.index.query.TermsQueryBuilder
import com.colasoft.opensearch.indexmanagement.rollup.randomTerms
import com.colasoft.opensearch.test.OpenSearchTestCase

class TermsTests : OpenSearchTestCase() {
    fun `test terms to bucket query has correct values`() {
        val terms = randomTerms()
        val randomKey = randomAlphaOfLengthBetween(1, 10)
        val bucketQuery = terms.toBucketQuery(randomKey) as TermsQueryBuilder

        Assert.assertTrue("Terms bucket query did not contain the correct key", bucketQuery.values().contains(randomKey))
        assertEquals("Terms bucket query did not contain the correct field name", bucketQuery.fieldName(), terms.sourceField)
    }
}
