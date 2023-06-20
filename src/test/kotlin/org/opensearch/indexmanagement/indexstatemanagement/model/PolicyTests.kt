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

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomState
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomTransition
import com.colasoft.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class PolicyTests : OpenSearchTestCase() {

    fun `test invalid default state`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for invalid default state") {
            randomPolicy().copy(defaultState = "definitely not this")
        }
    }

    fun `test empty states`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for empty states") {
            randomPolicy().copy(states = emptyList())
        }
    }

    fun `test duplicate states`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for duplicate states") {
            val states = listOf(randomState(name = "duplicate"), randomState(), randomState(name = "duplicate"))
            randomPolicy(states = states)
        }
    }

    fun `test transition pointing to nonexistent state`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for nonexistent transition state") {
            val states = listOf(randomState(transitions = listOf(randomTransition(stateName = "doesnt exist"))), randomState(), randomState())
            randomPolicy(states = states)
        }
    }
}
