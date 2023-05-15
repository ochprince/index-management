/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.opensearchapi

import org.junit.Assert
import com.colasoft.opensearch.OpenSearchException
import com.colasoft.opensearch.indexmanagement.util.IndexManagementException
import com.colasoft.opensearch.rest.RestStatus
import com.colasoft.opensearch.tasks.TaskCancelledException
import com.colasoft.opensearch.test.OpenSearchTestCase

class ExtensionsTests : OpenSearchTestCase() {

    fun `test is transform operation timeout`() {
        val ex = OpenSearchException(
            "opensearch test exception",
            TaskCancelledException("cancelled task with reason: Cancellation timeout of 100s is expired")
        )
        val result = isTransformOperationTimedOut(ex)
        Assert.assertTrue(result)
    }

    fun `test is transform operation timeout bad message`() {
        val result = isTransformOperationTimedOut(
            OpenSearchException(
                "opensearch test exception",
                TaskCancelledException("some test msg")
            )
        )
        Assert.assertFalse(result)
    }

    fun `test is retryable`() {
        Assert.assertTrue(isRetryable(IndexManagementException("502", RestStatus.BAD_GATEWAY, RuntimeException()), emptyList()))
        val ex = OpenSearchException(
            "opensearch test exception",
            TaskCancelledException("cancelled task with reason: Cancellation timeout of 100s is expired")
        )
        Assert.assertTrue(isRetryable(ex, emptyList()))
    }
}
