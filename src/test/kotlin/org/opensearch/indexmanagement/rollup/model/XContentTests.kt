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

import com.colasoft.opensearch.common.xcontent.LoggingDeprecationHandler
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentType
import com.colasoft.opensearch.indexmanagement.common.model.dimension.Dimension
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.rollup.model.metric.Metric
import com.colasoft.opensearch.indexmanagement.rollup.randomAverage
import com.colasoft.opensearch.indexmanagement.rollup.randomDateHistogram
import com.colasoft.opensearch.indexmanagement.rollup.randomHistogram
import com.colasoft.opensearch.indexmanagement.rollup.randomISMRollup
import com.colasoft.opensearch.indexmanagement.rollup.randomMax
import com.colasoft.opensearch.indexmanagement.rollup.randomMin
import com.colasoft.opensearch.indexmanagement.rollup.randomRollup
import com.colasoft.opensearch.indexmanagement.rollup.randomRollupMetrics
import com.colasoft.opensearch.indexmanagement.rollup.randomSum
import com.colasoft.opensearch.indexmanagement.rollup.randomTerms
import com.colasoft.opensearch.indexmanagement.rollup.randomValueCount
import com.colasoft.opensearch.indexmanagement.rollup.toJsonString
import com.colasoft.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class XContentTests : OpenSearchTestCase() {

    fun `test invalid dimension parsing`() {
        assertFailsWith(IllegalArgumentException::class, "Invalid dimension type [invalid_dimension] found in dimensions") {
            Dimension.parse(parser("{\"invalid_dimension\":{}}"))
        }
    }

    fun `test date histogram dimension parsing`() {
        val dateHistogram = randomDateHistogram()
        val dateHistogramString = dateHistogram.toJsonString()
        val parsedDateHistogram = Dimension.parse(parser(dateHistogramString))
        assertEquals("Round tripping Date Histogram doesn't work", dateHistogram, parsedDateHistogram)
    }

    fun `test histogram dimension parsing`() {
        val histogram = randomHistogram()
        val histogramString = histogram.toJsonString()
        val parsedHistogram = Dimension.parse(parser(histogramString))
        assertEquals("Round tripping Histogram doesn't work", histogram, parsedHistogram)
    }

    fun `test terms dimension parsing`() {
        val terms = randomTerms()
        val termsString = terms.toJsonString()
        val parsedTerms = Dimension.parse(parser(termsString))
        assertEquals("Round tripping Terms doesn't work", terms, parsedTerms)
    }

    fun `test invalid metric parsing`() {
        assertFailsWith(IllegalArgumentException::class, "Invalid metric type: [invalid_metric] found in rollup metrics") {
            Metric.parse(parser("{\"invalid_metric\":{}}"))
        }
    }

    fun `test average metric parsing`() {
        val avg = randomAverage()
        val avgString = avg.toJsonString()
        val parsedAvg = Metric.parse(parser(avgString))
        assertEquals("Round tripping Avg doesn't work", avg, parsedAvg)
    }

    fun `test max metric parsing`() {
        val max = randomMax()
        val maxString = max.toJsonString()
        val parsedMax = Metric.parse(parser(maxString))
        assertEquals("Round tripping Max doesn't work", max, parsedMax)
    }

    fun `test min metric parsing`() {
        val min = randomMin()
        val minString = min.toJsonString()
        val parsedMin = Metric.parse(parser(minString))
        assertEquals("Round tripping Min doesn't work", min, parsedMin)
    }

    fun `test sum metric parsing`() {
        val sum = randomSum()
        val sumString = sum.toJsonString()
        val parsedSum = Metric.parse(parser(sumString))
        assertEquals("Round tripping Sum doesn't work", sum, parsedSum)
    }

    fun `test value_count metric parsing`() {
        val valueCount = randomValueCount()
        val valueCountString = valueCount.toJsonString()
        val parsedValueCount = Metric.parse(parser(valueCountString))
        assertEquals("Round tripping ValueCount doesn't work", valueCount, parsedValueCount)
    }

    fun `test rollup metrics parsing`() {
        val rollupMetrics = randomRollupMetrics()
        val rollupMetricsString = rollupMetrics.toJsonString()
        val parsedRollupMetrics = RollupMetrics.parse(parser(rollupMetricsString))
        assertEquals("Round tripping RollupMetrics doesn't work", rollupMetrics, parsedRollupMetrics)
    }

    fun `test rollup parsing with type`() {
        val rollup = randomRollup().copy(delay = randomLongBetween(0, 60000000))
        val rollupString = rollup.toJsonString()
        val parser = parserWithType(rollupString)
        val parsedRollup = parser.parseWithType(rollup.id, rollup.seqNo, rollup.primaryTerm, Rollup.Companion::parse)
        // roles are deprecated and not populated in toXContent and parsed as part of parse
        assertEquals("Round tripping Rollup with type doesn't work", rollup.copy(roles = listOf()), parsedRollup)
    }

    fun `test rollup parsing without type`() {
        val rollup = randomRollup().copy(delay = randomLongBetween(0, 60000000))
        val rollupString = rollup.toJsonString(XCONTENT_WITHOUT_TYPE)
        val parsedRollup = Rollup.parse(parser(rollupString), rollup.id, rollup.seqNo, rollup.primaryTerm)
        // roles are deprecated and not populated in toXContent and parsed as part of parse
        assertEquals("Round tripping Rollup without type doesn't work", rollup.copy(roles = listOf()), parsedRollup)
    }

    fun `test ism rollup parsing`() {
        val ismRollup = randomISMRollup()
        val ismRollupString = ismRollup.toJsonString()
        val parsedISMRollup = ISMRollup.parse(parser(ismRollupString))
        assertEquals("Round tripping ISMRollup doesn't work", ismRollup, parsedISMRollup)
    }

    private fun parser(xc: String): XContentParser {
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
        parser.nextToken()
        return parser
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }
}
