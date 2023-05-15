/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.actionfilter

import com.colasoft.opensearch.action.fieldcaps.FieldCapabilitiesResponse
import com.colasoft.opensearch.indexmanagement.rollup.randomISMFieldCaps
import com.colasoft.opensearch.test.OpenSearchTestCase

class SerDeTests : OpenSearchTestCase() {

    fun `test round trip empty`() {
        val fieldCaps = FieldCapabilitiesResponse(arrayOf(), mapOf())
        val roundTripFromFieldCaps = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(fieldCaps).toFieldCapabilitiesResponse()
        assertEquals("Round tripping didn't work", fieldCaps, roundTripFromFieldCaps)
    }

    fun `test round trip nonempty`() {
        val ismFieldCaps = randomISMFieldCaps()
        val fieldCaps = ismFieldCaps.toFieldCapabilitiesResponse()
        val roundTrippedFieldCaps = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(fieldCaps).toFieldCapabilitiesResponse()
        assertEquals("Round tripping didn't work", fieldCaps, roundTrippedFieldCaps)
        assertEquals("Expected indices are different", ismFieldCaps.indices.size, roundTrippedFieldCaps.indices.size)
        assertEquals("Expected response map is different", ismFieldCaps.responseMap.size, roundTrippedFieldCaps.get().size)
    }
}
