/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.model

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.colasoft.opensearch.indexmanagement.opensearchapi.parseWithType
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.parser
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.randomNotificationConfig
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.toJsonString
import com.colasoft.opensearch.test.OpenSearchTestCase

class XContentTests : OpenSearchTestCase() {

    fun `test sm policy parsing`() {
        val smPolicy = randomSMPolicy(notificationConfig = randomNotificationConfig())
        val smPolicyString = smPolicy.toJsonString()
        val parsedSMPolicy = smPolicyString.parser().parseWithType(smPolicy.id, smPolicy.seqNo, smPolicy.primaryTerm, SMPolicy.Companion::parse)
        assertEquals("Round tripping sm policy with type doesn't work", smPolicy, parsedSMPolicy)
    }

    fun `test sm policy parsing without type`() {
        val smPolicy = randomSMPolicy(notificationConfig = randomNotificationConfig())
        val smPolicyString = smPolicy.toJsonString(XCONTENT_WITHOUT_TYPE)
        val parsedSMPolicy = SMPolicy.parse(smPolicyString.parser(), smPolicy.id, smPolicy.seqNo, smPolicy.primaryTerm)
        assertEquals("Round tripping sm policy without type doesn't work", smPolicy, parsedSMPolicy)
    }

    fun `test sm metadata parsing`() {
        val smMetadata = randomSMMetadata()
        val smMetadataString = smMetadata.toJsonString()
        val parsedSMMetadata = smMetadataString.parser().parseWithType(parse = SMMetadata.Companion::parse)
        assertEquals("Round tripping sm metadata doesn't work", smMetadata, parsedSMMetadata)
    }

    // TODO SM test object to json string matches expected
}
