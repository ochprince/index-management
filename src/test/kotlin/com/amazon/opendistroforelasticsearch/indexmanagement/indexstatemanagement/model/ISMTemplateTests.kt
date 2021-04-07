package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomISMTemplate
import org.opensearch.common.io.stream.InputStreamStreamInput
import org.opensearch.common.io.stream.OutputStreamStreamOutput
import org.opensearch.test.OpenSearchTestCase
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
