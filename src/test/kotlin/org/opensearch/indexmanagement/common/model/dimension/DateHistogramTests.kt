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

import org.junit.Assert
import com.colasoft.opensearch.index.query.RangeQueryBuilder
import com.colasoft.opensearch.indexmanagement.randomInstant
import com.colasoft.opensearch.indexmanagement.rollup.randomCalendarDateHistogram
import com.colasoft.opensearch.indexmanagement.rollup.randomDateHistogram
import com.colasoft.opensearch.indexmanagement.rollup.randomFixedDateHistogram
import com.colasoft.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval
import com.colasoft.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class DateHistogramTests : OpenSearchTestCase() {
    fun `test fixed date histogram to bucket query has correct values`() {
        val randomTime = randomLong()
        val dateHistogram = randomFixedDateHistogram()
        val dateHistogramInterval = DateHistogramInterval(dateHistogram.fixedInterval).estimateMillis()
        val bucketQuery = dateHistogram.toBucketQuery(randomTime) as RangeQueryBuilder

        assertEquals("Date histogram bucket query did not contain the correct timezone", dateHistogram.timezone.toString(), bucketQuery.timeZone())
        assertEquals("Date histogram bucket query did not contain the correct interval", dateHistogramInterval, bucketQuery.to() as Long - bucketQuery.from() as Long)
        assertEquals("Date histogram bucket query did not contain the correct bucket start time", randomTime, bucketQuery.from() as Long)
        assertEquals("Date histogram bucket query did not contain the correct format", "epoch_millis", bucketQuery.format())
        assertEquals("Date histogram bucket query did not contain the correct field name", bucketQuery.fieldName(), dateHistogram.sourceField)
        Assert.assertTrue("Date histogram bucket query should include the lower bounds", bucketQuery.includeLower())
        Assert.assertFalse("Date histogram bucket query should not include the upper bounds", bucketQuery.includeUpper())
    }

    fun `test calendar histogram to bucket query has correct values`() {
        val randomTime = randomLong()
        val dateHistogram = randomCalendarDateHistogram()
        val dateHistogramInterval = DateHistogramInterval(dateHistogram.calendarInterval).estimateMillis()
        val bucketQuery = dateHistogram.toBucketQuery(randomTime) as RangeQueryBuilder

        assertEquals("Date histogram bucket query did not contain the correct timezone", dateHistogram.timezone.toString(), bucketQuery.timeZone())
        assertEquals("Date histogram bucket query did not contain the correct interval", dateHistogramInterval, bucketQuery.to() as Long - bucketQuery.from() as Long)
        assertEquals("Date histogram bucket query did not contain the correct bucket start time", randomTime, bucketQuery.from() as Long)
        assertEquals("Date histogram bucket query did not contain the correct format", "epoch_millis", bucketQuery.format())
        assertEquals("Date histogram bucket query did not contain the correct field name", bucketQuery.fieldName(), dateHistogram.sourceField)
        Assert.assertTrue("Date histogram bucket query should include the lower bounds", bucketQuery.includeLower())
        Assert.assertFalse("Date histogram bucket query should not include the upper bounds", bucketQuery.includeUpper())
    }

    fun `test date histogram to bucket query fails with wrong bucket key type`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException with type Int when Long is expected") {
            randomDateHistogram().toBucketQuery(randomInt())
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException with type Instant when Long is expected") {
            randomDateHistogram().toBucketQuery(randomInstant())
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException with type Double when Long is expected") {
            randomDateHistogram().toBucketQuery(randomDouble())
        }
    }
}
