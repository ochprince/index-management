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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.coordinator

import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.core.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.util.NO_ID
import java.io.IOException

/**
 * Data class to hold partial [ManagedIndexConfig] data.
 *
 * This data class is used in the [com.colasoft.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator]
 * to hold partial data when reading in the [ManagedIndexConfig] document from the index.
 */
data class SweptManagedIndexConfig(
    val index: String,
    val seqNo: Long,
    val primaryTerm: Long,
    val uuid: String,
    val policyID: String,
    val policy: Policy?,
    val changePolicy: ChangePolicy?
) {

    companion object {
        @Suppress("ComplexMethod", "UNUSED_PARAMETER")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID, seqNo: Long, primaryTerm: Long): SweptManagedIndexConfig {
            lateinit var index: String
            lateinit var uuid: String
            lateinit var policyID: String
            var policy: Policy? = null
            var changePolicy: ChangePolicy? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndexConfig.INDEX_FIELD -> index = xcp.text()
                    ManagedIndexConfig.INDEX_UUID_FIELD -> uuid = xcp.text()
                    ManagedIndexConfig.POLICY_ID_FIELD -> policyID = xcp.text()
                    ManagedIndexConfig.POLICY_FIELD -> {
                        policy = if (xcp.currentToken() == Token.VALUE_NULL) null else Policy.parse(xcp)
                    }
                    ManagedIndexConfig.CHANGE_POLICY_FIELD -> {
                        changePolicy = if (xcp.currentToken() == Token.VALUE_NULL) null else ChangePolicy.parse(xcp)
                    }
                }
            }

            return SweptManagedIndexConfig(
                index,
                seqNo,
                primaryTerm,
                uuid,
                policyID,
                policy,
                changePolicy
            )
        }
    }
}
