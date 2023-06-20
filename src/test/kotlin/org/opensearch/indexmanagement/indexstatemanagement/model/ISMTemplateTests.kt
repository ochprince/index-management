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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.model

import com.colasoft.opensearch.common.io.stream.InputStreamStreamInput
import com.colasoft.opensearch.common.io.stream.OutputStreamStreamOutput
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomISMTemplate
import com.colasoft.opensearch.test.OpenSearchTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class ISMTemplateTests : OpenSearchTestCase() {

    fun `test basic`() {
        val expectedISMTemplate = randomISMTemplate()

        roundTripISMTemplate(expectedISMTemplate)
    }

    private fun roundTripISMTemplate(expectedISMTemplate: ISMTemplate) {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        expectedISMTemplate.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))

        val actualISMTemplate = ISMTemplate(input)
        assertEquals(expectedISMTemplate, actualISMTemplate)
    }
}
