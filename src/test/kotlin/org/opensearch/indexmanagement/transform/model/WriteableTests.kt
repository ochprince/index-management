/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.transform.model

import com.colasoft.opensearch.common.io.stream.BytesStreamOutput
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.indexmanagement.transform.buildStreamInputForTransforms
import com.colasoft.opensearch.indexmanagement.transform.randomTransform
import com.colasoft.opensearch.indexmanagement.transform.randomTransformMetadata
import com.colasoft.opensearch.test.OpenSearchTestCase

class WriteableTests : OpenSearchTestCase() {

    fun `test transform metadata as stream`() {
        val transformMetadata = randomTransformMetadata()
        val out = BytesStreamOutput().also { transformMetadata.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedTransformMetadata = TransformMetadata(sin)
        assertEquals("Round tripping TransformMetadata stream doesn't work", transformMetadata, streamedTransformMetadata)
    }

    fun `test transform as stream`() {
        val transform = randomTransform()
        val out = BytesStreamOutput().also { transform.writeTo(it) }
        val streamedTransform = Transform(buildStreamInputForTransforms(out))
        assertEquals("Round tripping Transform stream doesn't work", transform, streamedTransform)
    }

    fun `test transform roles field deprecation`() {
        val transform = randomTransform().copy(roles = listOf("I am deprecated"))
        val out = BytesStreamOutput().also { transform.writeTo(it) }
        val streamedTransform = Transform(buildStreamInputForTransforms(out))
        @Suppress("DEPRECATION")
        assertTrue("roles field in transform model is deprecated and should be parsed to empty list.", streamedTransform.roles.isEmpty())
    }
}
